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

	//Read the header field sequentially. Each call to readBytes
		message_size, err := ReadBytes(conn, 4)
		if err != nil { return err }

		request_api_key, err := ReadBytes(conn, 2)
		if err != nil { return err }

		api_version_bytes, err := ReadBytes(conn, 2)
		if err != nil { return err }

		correlational_id_bytes, err := ReadBytes(conn, 4)
		if err != nil {  
			return err
		}

		//convert to int the size of message
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
	fmt.Printf("Parsed topic name: '%s'\n", topicName)
	fmt.Printf("Request body length: %d\n", len(request_body))

	// Build the DescribeTopicPartitions v0 response body.
	var responseBody []byte

	// 1. throttle_time_ms (INT32)
	throttleTimeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(throttleTimeBytes, 0)
	responseBody = append(responseBody, throttleTimeBytes...)

	// 2. topics (ARRAY of Topic structs) - v0 uses regular arrays, not compact
	// Array has 1 element
	topicsArrayLength := make([]byte, 4)
	binary.BigEndian.PutUint32(topicsArrayLength, 1) // 1 topic
	responseBody = append(responseBody, topicsArrayLength...)

	// The Topic Struct:
	// error_code (INT16)
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, 3) // UNKNOWN_TOPIC_OR_PARTITION
	responseBody = append(responseBody, errorCodeBytes...)

	// name (STRING) - v0 uses regular strings, not compact
	topicNameBytes := encodeString(topicName)
	responseBody = append(responseBody, topicNameBytes...)

	// topic_id (UUID) - 16 bytes, all zeros for unknown topic
	topicIdBytes := make([]byte, 16) // Null UUID (all zeros)
	responseBody = append(responseBody, topicIdBytes...)

	// is_internal (BOOLEAN)
	responseBody = append(responseBody, byte(0)) // is_internal = false

	// partitions (ARRAY) - empty array for unknown topic
	partitionsArrayLength := make([]byte, 4)
	binary.BigEndian.PutUint32(partitionsArrayLength, 0) // 0 partitions
	responseBody = append(responseBody, partitionsArrayLength...)

	// topic_authorized_operations (INT32) - -1 for unknown
	authorizedOpsBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(authorizedOpsBytes, 0xFFFFFFFF) // -1 (unknown)
	responseBody = append(responseBody, authorizedOpsBytes...)

	// 3. next_cursor (nullable struct) - NULL for v0
	// In Kafka protocol, nullable structs use byte 0x00 for NULL
	responseBody = append(responseBody, byte(0x00)) // NULL cursor

	fmt.Printf("Response body length: %d\n", len(responseBody))
	
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
	
	fmt.Printf("Request body hex: %x\n", requestBody)
	fmt.Printf("Request body length: %d\n", len(requestBody))

	// Skip any leading tag fields (0x00)
	for offset < len(requestBody) && requestBody[offset] == 0x00 {
		offset++
	}
	
	if offset >= len(requestBody) {
		fmt.Printf("No data after tag fields\n")
		return ""
	}
	
	// Try to read the topics array
	// First, let's see if this is a COMPACT_STRING (topic name)
	topicNameLengthRaw, bytesRead := binary.Uvarint(requestBody[offset:])
	if bytesRead <= 0 {
		fmt.Printf("Error reading first VARINT\n")
		return ""
	}
	
	topicNameLength := int(topicNameLengthRaw) - 1 // Subtract 1 for compact string
	fmt.Printf("First VARINT: %d, interpreted as topic name length: %d\n", topicNameLengthRaw, topicNameLength)
	offset += bytesRead
	
	if topicNameLength > 0 && len(requestBody) >= offset+topicNameLength {
		topicName := string(requestBody[offset : offset+topicNameLength])
		fmt.Printf("Topic name: '%s'\n", topicName)
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