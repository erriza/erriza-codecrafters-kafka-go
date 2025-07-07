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
			// Prepare the error response
			responseSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(responseSizeBytes, 6) // Size of correlation_id (4) + error_code (2) = 6

			errorCodeBytes := make([]byte, 2)
			binary.BigEndian.PutUint16(errorCodeBytes, 35) // Error code for UNSUPPORTED_VERSION

			conn.Write(responseSizeBytes)
			conn.Write(correlational_id_bytes) // Use the slice we read directly
			conn.Write(errorCodeBytes)

    	} else {
			errorCodeBytes := make([]byte, 2) // [0, 0]
			binary.BigEndian.PutUint16(errorCodeBytes, 0)
					
			throttleTimeBytes := make([]byte, 4) // [0, 0, 0, 0]
			binary.BigEndian.PutUint32(throttleTimeBytes, 0)
					
			// 2. Build the ApiKeys array.
			// It contains one element, so its length is N+1 = 2.
			apiKeysArrayLength := []byte{2}
					
			// Build the single element for the array (the ApiVersions API struct)
			apiKey := make([]byte, 2) // [0, 18]
			binary.BigEndian.PutUint16(apiKey, 18)
					
			minVersion := make([]byte, 2) // [0, 0]
			binary.BigEndian.PutUint16(minVersion, 0)
					
			maxVersion := make([]byte, 2) // [0, 4]
			binary.BigEndian.PutUint16(maxVersion, 4)
					
			// The struct itself has a tagged fields section at the end. It's empty (length 0).
			structTaggedFields := []byte{0} // [0]
					
			// 3. The entire response message has a tagged fields section at the very end. It's also empty.
			responseTaggedFields := []byte{0} // [0]
					
			// 4. Combine everything in the correct order.
			var responseBody []byte
			responseBody = append(responseBody, errorCodeBytes...)       // 2 bytes
			responseBody = append(responseBody, throttleTimeBytes...)    // 4 bytes
			responseBody = append(responseBody, apiKeysArrayLength...)   // 1 byte
			// -- Start of array data --
			responseBody = append(responseBody, apiKey...)               // 2 bytes
			responseBody = append(responseBody, minVersion...)           // 2 bytes
			responseBody = append(responseBody, maxVersion...)           // 2 bytes
			responseBody = append(responseBody, structTaggedFields...)   // 1 byte
			// -- End of array data --
			responseBody = append(responseBody, responseTaggedFields...) // 1 byte
			// TOTAL BODY SIZE = 2+4+1+2+2+2+1+1 = 15 bytes
					
			// 5. Calculate the total message size.
			// correlation_id (4) + responseBody (15) = 19
			totalResponseSize := int32(len(correlational_id_bytes) + len(responseBody))
			responseSizeBytes := make([]byte, 4)
			binary.BigEndian.PutUint32(responseSizeBytes, uint32(totalResponseSize))
					
			// 6. Write the response.
			conn.Write(responseSizeBytes)
			conn.Write(correlational_id_bytes)
			conn.Write(responseBody)
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
