package main

import (
	"encoding/binary"
	"fmt"
	"io"
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
	// var correlational_id int32 = 7

	fmt.Println("Logs from your program will appear here!")
	
	l, err := net.Listen("tcp", "0.0.0.0:9092")
	if err != nil {
		fmt.Println("Failed to bind to port 9092")
		os.Exit(1)
	}
	// buf := make([]byte, 1024)
	for {
		conn, err := l.Accept()
		if err != nil {
			fmt.Println("Error accepting connection: ", err.Error())
			os.Exit(1)
		}

		// n, err := conn.Read(buf)
		// if err == io.EOF {
		// 	break
		// }
		// if err != nil {
		// 	fmt.Println("error reading", err)
		// 	return
		// }
		
		correlational_id := ReadCorrelationID(conn, 8)
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

func ReadCorrelationID(conn net.Conn, offset int64) int32 {
	buff := make([]byte, offset)

	_, err := io.ReadFull(conn, buff)
	if err != nil {
		fmt.Println("error reading and skipping offset", err)
		return 0
	}

	corrIDbuff := make([]byte, 4)
	_, err2 := io.ReadFull(conn, corrIDbuff)
	if err2 != nil {
		fmt.Println("error reading and skipping offset", err)
		return 0
	}
	
	correlationalId := binary.BigEndian.Uint32(corrIDbuff)
	return int32(correlationalId)
}
// 00000023 0012 0004 6f7fc661 00096b61666b612d636c69000a6b61666b612d636c6904302e3100