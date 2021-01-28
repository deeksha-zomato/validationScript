package main

import (
    "context"
    "log"
    "fmt"
    "time"

    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
    "go.mongodb.org/mongo-driver/bson"
)

var DATABASE string = "db"
var COLLECTION string = "auditTable"
var connectionURI string = "mongodb://localhost:27017/"
var lowerLimit int64 = 1511144000000
var upperLimit int64 = 1711348800000

var orderRoutines map[int32] int32
var orderLastState map[int32] int32
var badOrders map[int32] string

func routine(orderId int32,confirmPartner string,statusCode int32,isRPF bool) {
    defer func() {
		if err := recover(); err != nil {
			fmt.Println(err)
		}
	}()
    if val, ok := orderRoutines[orderId]; !ok {
        val=0
        orderRoutines[orderId] = val
    }
    orderRoutines[orderId]++
    //time.Sleep(10 * time.Millisecond)
    for {
        if orderRoutines[orderId]>1 {
            time.Sleep(1 * time.Millisecond)
        }else if isRPF{
            if statusCode == 80{
            }else if orderLastState[orderId] == 10 {
                if statusCode == 30{
                    orderLastState[orderId] = 30
                }else{
                     if value, ok := badOrders[orderId]; !ok {
                         value = confirmPartner
                         badOrders[orderId] = value
                     }
                }
            }else if orderLastState[orderId] == 40 {
                 if statusCode == 20{
                     orderLastState[orderId] = 20
                 }else{
                      if value, ok := badOrders[orderId]; !ok {
                          value = confirmPartner
                          badOrders[orderId] = value
                      }
                 }
             }else if orderLastState[orderId] == 20 {
                  if statusCode == 50{
                      orderLastState[orderId] = 50
                  }else{
                       if value, ok := badOrders[orderId]; !ok {
                           value = confirmPartner
                           badOrders[orderId] = value
                       }
                  }
              }else if orderLastState[orderId] == statusCode-10 {
                   orderLastState[orderId]= statusCode
               }else if _, found := badOrders[orderId]; found {

               }else{
                   if value, ok := badOrders[orderId]; !ok {
                       value = confirmPartner
                       badOrders[orderId] = value
                   }
               }
               break

        }else{
            if val,ok := orderLastState[orderId];!ok {
                if statusCode == 10{
                    val=10
                    orderLastState[orderId] = val
                }else{
                    if value, okay := badOrders[orderId]; !okay {
                        value = confirmPartner
                        badOrders[orderId] = value
                    }
                }
            }else if statusCode == 80{

            }else if orderLastState[orderId] == statusCode-10 {
                orderLastState[orderId]= statusCode
            }else if _, found := badOrders[orderId]; found {

            }else{
                if value, ok := badOrders[orderId]; !ok {
                    value = confirmPartner
                    badOrders[orderId] = value
                }
            }
            break
        }
    }
    orderRoutines[orderId]--
}


func main(){
    orderRoutines = make(map[int32] int32)
    orderLastState = make(map[int32] int32)
    badOrders = make(map[int32] string)
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
        { "doc_meta.created_at", bson.D{
             {"$gte",lowerLimit},
             {"$lte",upperLimit},
        }},
    },opts)

    defer sortCursor.Close(ctx)

   // var total int64 = 0
    for sortCursor.Next(ctx) {
        var orders bson.M
        if err = sortCursor.Decode(&orders); err!= nil {
            log.Fatal(err)
        }
        var status_code int32 = orders["state"].(int32)
        var reference_id int32 = orders["ref_id"].(int32)
        var order bson.M = (orders["order"].(interface{})).(bson.M)
        var is_rpf bool = order["is_rpf"].(bool)

        var confirm_partner = orders["confirm_partner"].(string)

        go routine(reference_id,confirm_partner,status_code,is_rpf)

    }

        for key,element := range badOrders {
             fmt.Println("OrderId:",key," confirmPartner:",element)
      }

      fmt.Printf("total number of bad orders under consideration: %d \n",len(badOrders))
      fmt.Printf("total number of orders under consideration: %d \n",len(orderLastState))


}