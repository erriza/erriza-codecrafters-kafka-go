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
		
		go handleConnection(conn)
	}	
}

func handleConnection(conn net.Conn) {
	defer conn.Close()
	
	for {
		err := handleReq(conn)
		if err != nil {
			fmt.Println("Error handling request", err)
			break
		}
	}
}

func handleReq(conn net.Conn) error {
	message_size, err := ReadBytes(conn, 4)
	if err != nil {
		return err // Return the error to stop the loop.
	}

	request_api_key, err := ReadBytes(conn, 2)
	if err != nil {
		return err
	}

	api_version_bytes, err := ReadBytes(conn, 2)
	if err != nil {
		return err
	}

	correlational_id_bytes, err := ReadBytes(conn, 4)
	if err != nil {
		return err
	}

	message_size_int := binary.BigEndian.Uint32(message_size)
	bytesRead := len(request_api_key) + len(api_version_bytes) + len(correlational_id_bytes)
	bytesLeftToRead := int(message_size_int) - bytesRead

	var requestBody []byte
	if bytesLeftToRead > 0 {
		requestBody, err = ReadBytes(conn, bytesLeftToRead)
		if err != nil {
			return err
		}
	}

	api_key := binary.BigEndian.Uint16(request_api_key)
	api_version := binary.BigEndian.Uint16(api_version_bytes)
	correlational_id := binary.BigEndian.Uint32(correlational_id_bytes)

	fmt.Println(api_key)
	fmt.Println(correlational_id)

	switch api_key {
	case 18:
		handleApiVersions(conn, api_version, correlational_id_bytes)
	case 75:
		handleDescribeTopicPartitions(conn, correlational_id_bytes, requestBody)
	default:
		fmt.Printf("Unsupported API Key: %d\n", api_key)
	}
	return nil
}

func handleApiVersions(conn net.Conn, api_version uint16, correlational_id_bytes []byte) {
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
            
            // ApiKeys is a COMPACT_ARRAY. Length is (N+1) as a VARINT.
            // To declare an array of N=2 element, the length is 3.
            apiKeysArrayLength := []byte{3} // [3] - means 2 element
            
            // API Key entry: api_key (18) + min_version (0) + max_version (4)
            apiKeyEntry := make([]byte, 6) // 2 + 2 + 2 = 6 bytes
            binary.BigEndian.PutUint16(apiKeyEntry[0:2], 18) // api_key = 18 (APIVersions)
            binary.BigEndian.PutUint16(apiKeyEntry[2:4], 0)  // min_version = 0
            binary.BigEndian.PutUint16(apiKeyEntry[4:6], 4)  // max_version = 4
            // Tagged fields for the API key entry (empty)
            apiKeyTaggedFields := []byte{0} // [0]
            
			//API KEY DESCRIBE TOPIC PARTITIONL: api_key (75) + min_version(0) + max_version(0)
			apiKeyEntry75 := make([]byte, 6)
			binary.BigEndian.PutUint16(apiKeyEntry75[0:2], 75)
			binary.BigEndian.PutUint16(apiKeyEntry75[2:4], 0)
			binary.BigEndian.PutUint16(apiKeyEntry75[4:6], 0)

			// Tagged fields for the API key entry75
			apiKeyTaggedFields75 := []byte{0}
            
            throttleTimeBytes := make([]byte, 4) // [0, 0, 0, 0]
            binary.BigEndian.PutUint32(throttleTimeBytes, 0)
            
            // Tagged fields for the response (empty)
            responseTaggedFields := []byte{0} // [0]
            
            // Build the complete response body
            var responseBody []byte
            responseBody = append(responseBody, errorCodeBytes...)        // error_code (2 bytes)
            responseBody = append(responseBody, apiKeysArrayLength...)    // num_of_api_keys (1 byte)
            responseBody = append(responseBody, apiKeyEntry...)           // api_key entry (6 bytes)
            responseBody = append(responseBody, apiKeyTaggedFields...)    // api_key tagged fields (1 byte)
			responseBody = append(responseBody, apiKeyEntry75...)
			responseBody = append(responseBody, apiKeyTaggedFields75...)

            responseBody = append(responseBody, throttleTimeBytes...)     // throttle_time_ms (4 bytes)
            responseBody = append(responseBody, responseTaggedFields...)  // response tagged fields (1 byte)
            
            // Calculate total message size: correlation_id (4) + responseBody
            totalResponseSize := int32(len(correlational_id_bytes) + len(responseBody))
            responseSizeBytes := make([]byte, 4)
            binary.BigEndian.PutUint32(responseSizeBytes, uint32(totalResponseSize))
            
            // Write the complete response
			// response  message size \ header \ body
			// message size 32 bit signed integer
			// var message_size int32 = 0
			// header correlational id
			// var correlational_id int32 = 7
            conn.Write(responseSizeBytes)
            conn.Write(correlational_id_bytes)
            conn.Write(responseBody)
		}
}

func handleDescribeTopicPartitions(conn net.Conn, correlational_id_bytes []byte, request_body []byte) {
	// Parse the v0 request body to get the topic name.
	topicName := parseDescribreTopicPartitionsRequest(request_body)

	// Build the full, flexible response body that the tester expects.
	var responseBody []byte

	// 1. throttle_time_ms (INT32) must be FIRST.
	throttleTimeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(throttleTimeBytes, 0)
	responseBody = append(responseBody, throttleTimeBytes...)

	// 2. topics (COMPACT_ARRAY of Topic structs)
	topicsArrayLength := []byte{2} // Length is N+1, so 1 element is length 2.
	responseBody = append(responseBody, topicsArrayLength...)

	// The full Topic Struct:
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, 3) // UNKNOWN_TOPIC_OR_PARTITION
	responseBody = append(responseBody, errorCodeBytes...)

	topicNameBytes := encodeCompactString(topicName) // Use COMPACT string for the response
	responseBody = append(responseBody, topicNameBytes...)

	topicIdBytes := make([]byte, 16) // Null UUID
	responseBody = append(responseBody, topicIdBytes...)

	responseBody = append(responseBody, byte(0)) // is_internal = false

	partitionsArrayLength := []byte{1} // Empty partitions array
	responseBody = append(responseBody, partitionsArrayLength...)

	authorizedOpsBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(authorizedOpsBytes, 0) // No authorized operations
	responseBody = append(responseBody, authorizedOpsBytes...)
	
	responseBody = append(responseBody, byte(0)) // Tagged fields for topic struct

	// 3. next_cursor (struct) is REQUIRED.
	responseBody = append(responseBody, byte(0)) // topic_name is a NULL compact string

	partitionIndexBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(partitionIndexBytes, 0xFFFFFFFF) // partition_index = -1
	responseBody = append(responseBody, partitionIndexBytes...)

	// 4. tagged_fields (for the whole response) is last.
	responseBody = append(responseBody, byte(0))

	// Calculate final size and send the complete response.
	totalResponseSize := int32(len(correlational_id_bytes) + len(responseBody))
	responseSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(responseSizeBytes, uint32(totalResponseSize))

	conn.Write(responseSizeBytes)
	conn.Write(correlational_id_bytes)
	conn.Write(responseBody)
}

func parseDescribreTopicPartitionsRequest(requestBody []byte) string {
	offset := 0

	// A v0 request body is an ARRAY of STRINGs.
	// An ARRAY starts with a 4-byte INT32 length.
	if len(requestBody) < offset+4 {
		return ""
	}
	topicsArrayLength := int(binary.BigEndian.Uint32(requestBody[offset : offset+4]))
	offset += 4

	if topicsArrayLength > 0 {
		// A v0 STRING starts with a 2-byte INT16 length.
		if len(requestBody) < offset+2 {
			return ""
		}
		topicNameLength := int(binary.BigEndian.Uint16(requestBody[offset : offset+2]))
		offset += 2

		// Read the topic name bytes.
		if len(requestBody) < offset+topicNameLength {
			return ""
		}
		topicName := string(requestBody[offset : offset+topicNameLength])
		return topicName
	}

	return ""
}

// Also update the response to ensure we return exactly 1 topic when we have a topic name
// The issue might be that we're always returning 1 topic regardless of whether we found one


func encodeString(s string) []byte {
	// Regular STRING format: 2-byte length + string data
	result := make([]byte, 2+len(s))
	binary.BigEndian.PutUint16(result[0:2], uint16(len(s)))
	copy(result[2:], []byte(s))
	return result
}

func encodeCompactString(s string) []byte {
	if s == "" {
		return []byte{1}
	}

	result := make([]byte, 1+len(s))
	result[0] = byte(len(s) + 1) // length + 1
	copy(result[1:], []byte(s))
	return  result
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

func readCompactString(buffer []byte, offset int) (string, int) {
	length, bytesRead := binary.Uvarint(buffer[offset:])
	offset+= bytesRead

	strLen := int(length-1)
	if strLen <= 0 {
		return "", offset
	}

	end := offset + strLen
	str := string(buffer[offset:end])
	return str, end
}

func readCompactArrayLength(buffer []byte, offset int)(int, int) {
	length, bytesRead := binary.Uvarint(buffer[offset:])
	offset+= bytesRead

	return int(length-1), offset
}