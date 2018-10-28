package main

import (
	"bufio"
	"fmt"
	"net"
	"os"
	"strings"
	"sync"
)

var database map[string]string
var mutex = &sync.Mutex{}

func main() {
	database = make(map[string]string)
	database["jackson"] = "dinky"

	arguments := os.Args
	if len(arguments) == 1 {
		fmt.Println("Please provide a port number!")
		return
	}

	PORT := ":" + arguments[1]
	l, err := net.Listen("tcp4", PORT)
	if err != nil {
		fmt.Println(err)
		return
	}
	defer l.Close()

	for {
		c, err := l.Accept()
		if err != nil {
			fmt.Println(err)
			return
		}
		go handleConnection(c)
	}

}

func handleConnection(c net.Conn) {
	fmt.Printf("Serving %s\n", c.RemoteAddr().String())
	transactionMap := make(map[string]string)
	var delKeys []string

	write(c, "===================================================================================\n"+
		"Commands:\n"+
		"BEGIN -> Starts a session.\n"+
		"GET <key> -> returns value for the provided key, if one exists.\n"+
		"SET <key> <value> -> creates a new entry with the provide key and value.\n"+
		"DEL <key> -> tries to delete the value at the given key.\n"+
		"COMMIT -> Saves all changes to the database.\n"+
		"QUIT -> Disconnect from the server.\n"+
		"===================================================================================\n")
	for {
		usrInput, err := bufio.NewReader(c).ReadString('\n')
		usrInput = strings.Replace(usrInput, "\n", "", -1)
		if err != nil {
			fmt.Println(err)
			return
		}

		quit := false
		fmt.Println(usrInput)

		switch usrInput {
		case "BEGIN":
			for k, v := range database {
				transactionMap[k] = v
			}
			quit = true
		default:
			write(c, "Please type BEGIN to start your session.")
		}

		if quit {
			break
		}
	}

	for {
		usrInput, err := bufio.NewReader(c).ReadString('\n')
		usrInput = strings.Replace(usrInput, "\n", "", -1)
		if err != nil {
			fmt.Println(err)
			return
		}

		quit := false
		var result string
		inputStrings := strings.Fields(string(usrInput))

		switch inputStrings[0] {
		case "GET":
			if len(inputStrings) != 2 {
				write(c, "Invalid GET command. Expecting two arguements i.e \"GET <key>\"")
			} else {
				result = transactionMap[inputStrings[1]]
			}
		case "SET":
			if len(inputStrings) != 3 {
				write(c, "Invalid SET command. Expecting three arguements i.e \"SET <key> <value>\"")
			} else {
				transactionMap[inputStrings[1]] = inputStrings[2]
				write(c, "OK")
			}
		case "DEL":
			if len(inputStrings) != 2 {
				write(c, "Invalid GET command. Expecting two arguements i.e \"GET <key>\"")
			} else {
				delete(transactionMap, inputStrings[1])
				delKeys = append(delKeys, inputStrings[1])
			}
		case "COMMIT":
			mutex.Lock()
			// Add new/changed keys/values to the database.
			for k, v := range transactionMap {
				if _, ok := database[k]; ok {
					if database[k] != transactionMap[k] {
						database[k] = transactionMap[k]
					}
				} else {
					database[k] = v
				}
			}

			// Delete removed keys from the database.
			for _, key := range delKeys {
				if _, ok := database[key]; ok {
					delete(database, key)
				}
			}
			mutex.Unlock()
		case "QUIT":
			quit = true
		default:
			write(c, "Please enter a valid command: GET, SET, DEL, QUIT")
		}

		if quit {
			break
		}

		write(c, result)
	}

	c.Close()
}

func write(c net.Conn, msg string) {
	c.Write([]byte(string(msg + "\n")))
}
