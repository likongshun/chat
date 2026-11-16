package main

import (
 "fmt"
 "net"
)
func main(){l,e:=net.Listen("tcp","127.0.0.1:1502");if e!=nil{panic(e)};defer l.Close();fmt.Println("Modbus TCP simulator socket listening on",l.Addr());for{c,e:=l.Accept();if e!=nil{return};go func(){defer c.Close();buf:=make([]byte,260);_,_=c.Read(buf)}()}}
