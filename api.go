package tmqp

import (
	"bufio"
	"encoding/binary"
	"fmt"
	"github.com/golang/protobuf/proto"
	"net"
	"os"
	"strings"
)

const (
	DELIMITER byte = '\n'
	TYPE      uint = 1
	SIZE      uint = 4
	VERSION        = "TMQP 0.1\n"
)

type ModelMessage struct {
	Queue   string
	Sender  string
	Content []byte
}

var messages = make(chan ModelMessage)

func Handshake() net.Conn {

	conn, err := net.Dial("tcp", ":7788")
	checkError(err)
	//defer conn.Close()

	handleConn(conn)

	return conn
}

func handleConn(conn net.Conn) {

	fmt.Println("STARTING CONNECTION!")

	conn.Write([]byte("TMQP 0.1\n"))

	msg, err := bufio.NewReader(conn).ReadString('\n')

	if strings.Compare(msg, "START\n") == 0 {
		_, err = conn.Write([]byte("START OK\n"))
	}

	checkError(err)
}

func checkError(err error) {
	if err != nil {
		fmt.Fprintf(os.Stderr, "Fatal error: %s", err.Error())
		os.Exit(1)
	}
}

func NewQueue(conn net.Conn, name string) {

	name = strings.TrimSuffix(name, "\n")
	strings.Replace(name, " ", "", -1)

	var type_package int8
	type_package = 1

	type_byte := make([]byte, TYPE)
	type_byte[0] = (byte(type_package))

	err := binary.Write(conn, binary.LittleEndian, type_byte)

	if err == nil {

		// fmt.Println("ENVIANDO PACOTE...")
		// fmt.Println("TYPE: ", type_byte)

		pacote := &QueueDeclare{
			Name:      name,
			Exclusive: false,
		}

		data, _ := proto.Marshal(pacote)

		size_package := uint32(len(data))
		size_bytes := make([]byte, SIZE)
		binary.LittleEndian.PutUint32(size_bytes, uint32(size_package))

		// fmt.Println("SIZE: ", size_bytes)

		err := binary.Write(conn, binary.LittleEndian, size_bytes)

		if err != nil {

			fmt.Println("ERROR - WRITE SIZE: ", err)

		} else {
			_, err := conn.Write(data)

			if err != nil {
				fmt.Println("ERROR - WRITE DATA: ", err)
			} else {
				fmt.Println("NOVA FILA CRIADA.")
			}

		}

	} else {
		fmt.Println("ERROR - WRITE TYPE: ", err)
	}
}

func Publish(conn net.Conn, sender string, queue string, content string) {

	sender = strings.TrimSuffix(sender, "\n")
	queue = strings.TrimSuffix(queue, "\n")
	content = strings.TrimSuffix(content, "\n")

	strings.Replace(sender, " ", "", -1)
	strings.Replace(queue, " ", "", -1)
	strings.Replace(content, " ", "", -1)

	var type_package int8
	type_package = 2

	type_byte := make([]byte, TYPE)
	type_byte[0] = (byte(type_package))

	err := binary.Write(conn, binary.LittleEndian, type_byte)

	content_bytes := make([]byte, 10000)
	content_bytes = []byte(content)

	if err == nil {

		// fmt.Println("ENVIANDO PACOTE...")
		// fmt.Println("TYPE: ", type_byte)

		pacote := &Message{
			Queue:   queue,
			Sender:  sender,
			Content: content_bytes,
		}

		data, _ := proto.Marshal(pacote)

		size_package := uint32(len(data))
		size_bytes := make([]byte, SIZE)
		binary.LittleEndian.PutUint32(size_bytes, uint32(size_package))

		// fmt.Println("SIZE: ", size_bytes)

		err := binary.Write(conn, binary.LittleEndian, size_bytes)

		if err != nil {

			fmt.Println("ERROR - WRITE SIZE: ", err)

		} else {
			_, err := conn.Write(data)

			if err != nil {
				fmt.Println("ERROR - WRITE DATA: ", err)
			} else {
				fmt.Println("MESSAGE PUBLISHED IN SERVER...")
			}

		}

	} else {
		fmt.Println("ERROR - WRITE TYPE: ", err)
	}

}

func Consume(conn net.Conn, queue string) chan ModelMessage {

	queue = strings.TrimSuffix(queue, "\n")
	strings.Replace(queue, " ", "", -1)

	var type_package int8
	type_package = 3

	type_byte := make([]byte, TYPE)
	type_byte[0] = (byte(type_package))

	err := binary.Write(conn, binary.LittleEndian, type_byte)

	if err == nil {

		// fmt.Println("ENVIANDO PACOTE...")
		// fmt.Println("TYPE: ", type_byte)

		pacote := &ConsumeQueue{
			Queue: queue,
		}

		data, _ := proto.Marshal(pacote)

		size_package := uint32(len(data))
		size_bytes := make([]byte, SIZE)
		binary.LittleEndian.PutUint32(size_bytes, uint32(size_package))

		// fmt.Println("SIZE: ", size_bytes)

		err := binary.Write(conn, binary.LittleEndian, size_bytes)

		if err != nil {

			fmt.Println("ERROR - WRITE SIZE: ", err)

		} else {
			_, err := conn.Write(data)

			if err != nil {
				fmt.Println("ERROR - WRITE DATA: ", err)
			} else {
				fmt.Println("SENDING NEW PACKET TO LISTEN ", queue)
				go consumeQueue(conn, queue)
			}

		}

	} else {
		fmt.Println("ERROR - WRITE TYPE: ", err)
	}

	return messages
}

func consumeQueue(conn net.Conn, queue string) {

	for {

		type_byte := make([]byte, TYPE)
		_, err := conn.Read(type_byte[0:TYPE])

		if err == nil {

			type_package := int(type_byte[0])

			size_bytes := make([]byte, SIZE)
			_, err := conn.Read(size_bytes[0:SIZE])

			if err != nil {
				fmt.Println("CONNECTION LOST WITH SERVER!")
				os.Exit(1)
			} else if type_package == 2 {

				size_package := binary.LittleEndian.Uint32(size_bytes[0:SIZE])

				content_bytes := make([]byte, size_package)

				// fmt.Println("SIZEx: ", size_package)

				_, err := conn.Read(content_bytes[0:size_package])

				if err != nil {
					fmt.Println("ERROR - CONTEUDO: ", err)
					os.Exit(1)
				} else {

					// messages <- content_bytes

					message := &Message{}
					err := proto.Unmarshal(content_bytes, message)

					// fmt.Println("CONTENT: ", message.GetContent())

					if err != nil {
						fmt.Println("ERROR - UNMARSHALx: ", err)
					} else if strings.Compare(message.GetQueue(), queue) == 0 {

						messages <- ModelMessage{Queue: message.GetQueue(), Sender: message.GetSender(), Content: message.GetContent()}
						// content_bytes := message.GetContent()
						// output := string(content_bytes)
						// fmt.Println("<= ", output)
						// fmt.Println(message.GetSender(), "<=", output)
						// fmt.Print("=> ")
					} else {
						fmt.Println("ERROR: QUEUE NOT FOUND!")
					}
				}
			}
		}
	}

}
