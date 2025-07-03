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
	// var message_size int32 = 0
	// header correlational id
	// var correlational_id int32 = 7

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
		

		//Read the header field sequentially. Each call to readBytes

		message_size, err := ReadBytes(conn, 4)
		if err != nil { continue }

		request_api_key, err := ReadBytes(conn, 2)
		if err != nil { continue }

		api_version_bytes, err := ReadBytes(conn, 2)
		if err != nil { continue }

		correlational_id_bytes, err := ReadBytes(conn, 4)
		if err != nil { continue }

		// message_size_bytes := Int32ToBytes(message_size, binary.BigEndian)
		api_version := binary.BigEndian.Uint16(api_version_bytes)

		correlational_id := binary.BigEndian.Uint32(correlational_id_bytes)

		// request_api_key_bytes := Int32ToBytes(request_api_key, binary.BigEndian)
		// correlational_id_bytes := Int32ToBytes(correlational_id, binary.BigEndian)


		fmt.Println(request_api_key)
		fmt.Println(correlational_id)


	    if api_version > 4 {
			// Prepare the error response
			messageSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(messageSizeBytes, 6) // Size of correlation_id (4) + error_code (2) = 6

			errorCodeBytes := make([]byte, 2)
			binary.BigEndian.PutUint16(errorCodeBytes, 35) // Error code for UNSUPPORTED_VERSION

			// Write the response: message_size + correlation_id + error_code
			conn.Write(message_size)
			conn.Write(correlational_id_bytes) // Use the slice we read directly
			conn.Write(errorCodeBytes)

    	} else {

			errCode := make([]byte, 2)
			binary.BigEndian.AppendUint16(errCode, 0)

			conn.Write(message_size)
			conn.Write(correlational_id_bytes) // Use the slice we read directly
			conn.Write(errCode) // 
		}
	}	
}

func Int32ToBytes(n int32, byteOrder binary.ByteOrder) []byte {
	buf := make([]byte, 4)
	byteOrder.PutUint32(buf, uint32(n))
	return buf
}

func ReadBytes(conn net.Conn, bytesToRead int) ([]byte, error) {
	//read first 8 bytes ignore them
	buff := make([]byte, bytesToRead)
	_, err := io.ReadFull(conn, buff)
	if err != nil {
		fmt.Println("error reading and skipping offset", err)
		return nil, err
	}
	return buff, nil
}

// func ReadApiVerson(conn net.Conn, offset int64) int32 {
// 	buff := make([]byte, offset)

// 	_, err := io.ReadFull(conn, buff) {
// 		if err != nil {
// 			fmt.Println("error reading skipping offset", err)
// 			return 0
// 		}
// 	}

// 	// read actual Api version

// }


// 00 00 00 23  00 12  67 4a  4f 74 d2 8b  00096b61666b612d636c69000a6b61666b612d636c6904302e3100