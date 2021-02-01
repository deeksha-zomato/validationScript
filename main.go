package main

import (
    "context"
    "log"
    "fmt"
    "time"
    "sync"
    "os"
    "strconv"

    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/bson"
)

type OrderStatus int

const (
	OsCreated         OrderStatus = 10
	OsConfirmed       OrderStatus = 20
	OsAssigned        OrderStatus = 30
	OsReachedPickup   OrderStatus = 40
	OsPickedUp        OrderStatus = 50
	OsReachedDrop     OrderStatus = 60
	OsCompleted       OrderStatus = 70
	OsCanceled        OrderStatus = 80
	OsReturnInit      OrderStatus = 90
	OsReturnAssigned  OrderStatus = 100
	OsReturnCompleted OrderStatus = 110
	OsReturnCancelled OrderStatus = 120
)


var DATABASE string = "db"
var COLLECTION string = "auditTable"
var connectionURI string = "mongodb://localhost:27017/"

var orderRoutines sync.Map
var orderLastState sync.Map
var badOrders sync.Map

func routine(orderId int32,confirmPartner string,statusCode OrderStatus,isRPF bool) {
    defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
    if _, ok := orderRoutines.Load(orderId); !ok {
        orderRoutines.Store(orderId,0)
    }
    if val, ok := orderRoutines.Load(orderId); ok {
        orderRoutines.Store(orderId,val.(int)+1)
    }
    for {
        if val,_ :=orderRoutines.Load(orderId); val.(int)>1 {
            time.Sleep(10 * time.Millisecond)
        }else if isRPF{
            if statusCode == OsCanceled {
            }else if val,_:=orderLastState.Load(orderId); val.(OrderStatus)==OsCreated{
                if statusCode == OsAssigned{
                    orderLastState.Store(orderId, OsAssigned)
                }else{
                     if _, ok := badOrders.Load(orderId); !ok {
                        badOrders.Store(orderId,confirmPartner)
                     }
                }
            }else if val,_:=orderLastState.Load(orderId); val.(OrderStatus)==OsReachedPickup{
                 if statusCode == OsConfirmed{
                     orderLastState.Store(orderId,OsConfirmed)
                 }else{
                      if _, ok := badOrders.Load(orderId); !ok {
                          badOrders.Store(orderId,confirmPartner)
                      }
                 }
            }else if val,_:=orderLastState.Load(orderId); val.(OrderStatus)==OsConfirmed{
                  if statusCode == OsPickedUp{
                      orderLastState.Store(orderId,OsPickedUp)
                  }else{
                       if _, ok := badOrders.Load(orderId); !ok {
                           badOrders.Store(orderId,confirmPartner)
                       }
                  }
            }else if val,_:=orderLastState.Load(orderId); val.(OrderStatus)==statusCode-10 {
                      orderLastState.Store(orderId,statusCode)
            }else if _, found := badOrders.Load(orderId); found {

            }else{
                   if _, ok := badOrders.Load(orderId); !ok {
                       badOrders.Store(orderId,confirmPartner)
                   }
            }
            break
        }else{
            if _,ok := orderLastState.Load(orderId);!ok {
                if statusCode == OsCreated{
                    orderLastState.Store(orderId, OsCreated)
                }else{
                    if _, ok := badOrders.Load(orderId); !ok {
                        badOrders.Store(orderId,confirmPartner)
                      }
                }
            }else if statusCode == OsCanceled{

            }else if val,_:=orderLastState.Load(orderId); val.(OrderStatus)==statusCode-10 {
                orderLastState.Store(orderId,statusCode)
            }else if _, found := badOrders.Load(orderId); found {

            }else{
                if _, ok := badOrders.Load(orderId); !ok {
                    badOrders.Store(orderId,confirmPartner)
                  }
            }
            break
        }
    }
    if val, ok := orderRoutines.Load(orderId); ok {
        orderRoutines.Store(orderId,val.(int)-1)
    }
}


func main(){
    lowerLimit,_ := strconv.ParseInt(os.Args[1],10,64)
    upperLimit,_ := strconv.ParseInt(os.Args[2],10,64)

    client, err := mongo.NewClient(options.Client().ApplyURI(connectionURI))
    if err!= nil{
        log.Fatal(err)
    }
    ctx, _ := context.WithTimeout(context.Background(),10*time.Second)
    err = client.Connect(ctx)
    if err!= nil{
          log.Fatal(err)
    }
    defer client.Disconnect(ctx)

    database := client.Database(DATABASE)
    ordersCollection := database.Collection(COLLECTION)

    opts := options.Find()
    opts.SetBatchSize(1000)
    opts.SetSort(bson.D{{"_id",1}})

    sortCursor, err := ordersCollection.Find(ctx,bson.D{
        // Include all the orders that are updated within the given time interval
        //and two hrs ahead of it
        { "doc_meta.updated_at", bson.D{
             {"$gte",lowerLimit},
             {"$lte",upperLimit+7200},
        }},
    },opts)

    defer sortCursor.Close(ctx)

    for sortCursor.Next(ctx) {
        var orders bson.M
        if err = sortCursor.Decode(&orders); err!= nil {
            log.Fatal(err)
        }
        var status_code OrderStatus = OrderStatus(orders["state"].(int32))
        var reference_id int32 = orders["ref_id"].(int32)
        var order bson.M = (orders["order"].(interface{})).(bson.M)
        var is_rpf bool = order["is_rpf"].(bool)
        var doc_meta bson.M = (orders["doc_meta"].(interface{})).(bson.M)
        var created_at int64 = doc_meta["created_at"].(int64)
        var confirm_partner = orders["confirm_partner"].(string)

        if created_at>=lowerLimit && created_at<=upperLimit {
            //Take the orders that are created within the given time interval
            go routine(reference_id,confirm_partner,status_code,is_rpf)
        }
    }

    cnt:=0 //counts the number of bad orders
    tot:=0 //counts the total number of orders
    badOrders.Range(func(key, value interface{}) bool {
        fmt.Println("referenceId:",key.(int32),"confirmPartner:",value.(string))
        cnt++
        return true
    })
    orderRoutines.Range(func(_, _ interface{}) bool {
        tot++
        return true
    })

    fmt.Printf("total number of bad orders under consideration: %d \n",cnt)
    fmt.Printf("total number of orders under consideration: %d \n",tot)

}