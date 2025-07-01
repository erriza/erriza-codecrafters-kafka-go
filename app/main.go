package main

import (
	"encoding/binary"
	"fmt"
	"net"
	"os"
)

// Ensures gofmt doesn't remove the "net" and "os" imports in stage 1 (feel free to remove this!)
var _ = net.Listen
var _ = os.Exit

func main() {
	// response  message size \ header \ body
	// message size 32 bit signed integer
	var message_size int32 = 0
	// header correlational id
	var correlational_id int32 = 7

	fmt.Println("Logs from your program will appear here!")
	
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}
		message_size_bytes := Int32ToBytes(message_size, binary.BigEndian)
		correlational_id_bytes := Int32ToBytes(correlational_id, binary.BigEndian)

		conn.Write(message_size_bytes)
		conn.Write(correlational_id_bytes)
	}	
}

func Int32ToBytes(n int32, byteOrder binary.ByteOrder) []byte {
	buf := make([]byte, 4)
	byteOrder.PutUint32(buf, uint32(n))
	return buf
}