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
	//Parse the DescribeTopicPartititions request body
	topicName := parseDescribreTopicPartitionsRequest(request_body)

	// Build the response
	var responseBody []byte
	
	// topics array - compact array with 1 element, so length is 2
	topicArrayLength := []byte{2}
	responseBody = append(responseBody, topicArrayLength...)

	// Topic entry 
	// error_code (2 bytes) - UNKNOWN_TOPIC_OR_PARTITION = 3
	errorCodeBytes := make([]byte, 2)
	binary.BigEndian.PutUint16(errorCodeBytes, 3)
	responseBody = append(responseBody, errorCodeBytes...)

	// topic_name (compact string)
	topicNameBytes := encodeCompactString(topicName)
	responseBody = append(responseBody, topicNameBytes...)
	
	// topic_id (16 bytes) - 00000000-0000-0000-0000-000000000000
	topicIdBytes := make([]byte, 16)
	responseBody = append(responseBody, topicIdBytes...)
	
	// is_internal (1 byte) - false
	responseBody = append(responseBody, byte(0))
	
	// partitions array - empty compact array, so length is 1
	partitionsArrayLength := []byte{1}
	responseBody = append(responseBody, partitionsArrayLength...)
	
	// topic_authorized_operations (4 bytes) - -2147483648 (not supported)
	authorizedOpsBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(authorizedOpsBytes, 0x80000000) // -2147483648
	responseBody = append(responseBody, authorizedOpsBytes...)
	
	// tagged fields for topic (empty)
	responseBody = append(responseBody, byte(0))

	// throttle_time_ms (4 bytes) - set to 0
	throttleTimeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(throttleTimeBytes, 0)
	responseBody = append(responseBody, throttleTimeBytes...)

	// nextCursorBytes := make([]byte, 4)
	// binary.BigEndian.PutUint32(nextCursorBytes, 0xFFFFFFFF) // -1
	// responseBody = append(responseBody, nextCursorBytes...)

	// tagged fields for response (empty)
	responseBody = append(responseBody, byte(0))
	
	// Calculate total message size: correlation_id (4) + responseBody
	totalResponseSize := int32(len(correlational_id_bytes) + len(responseBody))
	responseSizeBytes := make([]byte, 4)
	binary.BigEndian.PutUint32(responseSizeBytes, uint32(totalResponseSize))

	conn.Write(responseSizeBytes)
	conn.Write(correlational_id_bytes)
	conn.Write(responseBody)
}

func parseDescribreTopicPartitionsRequest(requestBody []byte) string {
	offset := 0

	topicsArrayLen, offset := readCompactArrayLength(requestBody, offset)

	if topicsArrayLen > 0 {
		topicName, _ := readCompactString(requestBody, offset)
		return topicName
	}
	return ""
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