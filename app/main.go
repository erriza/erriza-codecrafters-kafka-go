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

		//convert to int the size of message
		message_size_int := binary.BigEndian.Uint32(message_size)

		bytesRead := len(request_api_key) + len(api_version_bytes) + len(correlational_id_bytes)

		bytesLeftToRead := int(message_size_int) - bytesRead


		if bytesLeftToRead > 0 {
			_,err := ReadBytes(conn, bytesLeftToRead)
			if err != nil {
				continue
			}
		}

		api_version := binary.BigEndian.Uint16(api_version_bytes)

		correlational_id := binary.BigEndian.Uint32(correlational_id_bytes)

		fmt.Println(request_api_key)
		fmt.Println(correlational_id)

	    if api_version > 4 {
			responseSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(responseSizeBytes, 6) // Size of correlation_id (4) + error_code (2) = 6

			errorCodeBytes := make([]byte, 2)
			binary.BigEndian.PutUint16(errorCodeBytes, 35) // Error code for UNSUPPORTED_VERSION

			conn.Write(responseSizeBytes)
			conn.Write(correlational_id_bytes)
			conn.Write(errorCodeBytes)

    	} else {
			errorCodeBytes := make([]byte, 2) // [0, 0]
			binary.BigEndian.PutUint16(errorCodeBytes, 0)

			throttleTimeBytes := make([]byte, 4) // [0, 0, 0, 0]
			binary.BigEndian.PutUint32(throttleTimeBytes, 0)

			// ApiKeys is a COMPACT_ARRAY. Length is (N+1) as a VARINT.
			// To declare an array of N=1 element, the length is 2.
			apiKeysArrayLength := []byte{2} // [2]
			apiStruct_ApiKey := make([]byte, 2)
			binary.BigEndian.PutUint16(apiStruct_ApiKey, 18)

			// The entire response message has a tagged fields section at the very end.
			// An empty one has a length of 0.
			responseTaggedFields := []byte{0} // [0]

			// 2. Combine the body parts. We DO NOT include the 7-byte struct.
			// Total body size = error(2) + throttle(4) + array_len(1) + tagged_fields(1) = 8 bytes.
			var responseBody []byte
			responseBody = append(responseBody, errorCodeBytes...)
			responseBody = append(responseBody, throttleTimeBytes...)
			responseBody = append(responseBody, apiKeysArrayLength...)
			// DO NOT APPEND THE STRUCT DATA HERE
			responseBody = append(responseBody, responseTaggedFields...)

			// 3. Calculate total message size.
			// correlation_id (4) + responseBody (8) = 12
			totalResponseSize := int32(len(correlational_id_bytes) + len(responseBody))
			responseSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(responseSizeBytes, uint32(totalResponseSize))

			// 4. Write the response.
			conn.Write(responseSizeBytes)
			conn.Write(correlational_id_bytes)
			conn.Write(responseBody)
			conn.Write(apiStruct_ApiKey)
		}
	}	
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

// func Int32ToBytes(n int32, byteOrder binary.ByteOrder) []byte {
// 	buf := make([]byte, 4)
// 	byteOrder.PutUint32(buf, uint32(n))
// 	return buf
// }
