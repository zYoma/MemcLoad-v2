package main

import (
	"MemcLoad/pkg/appsinstalled"
	"bufio"
	"compress/gzip"
	"flag"
	"fmt"
	"github.com/bradfitz/gomemcache/memcache"
	"github.com/golang/protobuf/proto"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type AppsInstalled struct {
	devType string
	devId   string
	lat     float64
	lon     float64
	apps    []uint32
}

var logPath string
var memcachedInstance *memcache.Client

func initializeMemcachedClients(connectionString string) *memcache.Client {
	memcachedInstance = memcache.New(connectionString)
	memcachedInstance.MaxIdleConns = 2
	memcachedInstance.Timeout = 100 * time.Millisecond

	return memcachedInstance
}

func main() {
	start := time.Now()
	// парсим аргумент командной строки с путем до директории логов
	flag.StringVar(&logPath, "logPath", "logs.tsv.gz", "Путь до директории с логами")
	flag.Parse()

	// создаем карту с клиентами memcache для каждого devType
	clients := make(map[string]*memcache.Client)
	clients["idfa"] = initializeMemcachedClients("127.0.0.1:33013")
	clients["gaid"] = initializeMemcachedClients("127.0.0.1:33014")
	clients["adid"] = initializeMemcachedClients("127.0.0.1:33015")
	clients["dvid"] = initializeMemcachedClients("127.0.0.1:33016")

	// получаем список файлов с логами из переданной директории
	files, err := ioutil.ReadDir(logPath)
	if err != nil {
		log.Fatal(err)
	}
	var result []ClientItem
	// обходим все файлы
	for _, f := range files {
		filePath := logPath + f.Name()
		// открываем файл
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatal("could not open the file: %v", err)
		}
		defer file.Close()

		reader, err := gzip.NewReader(file)
		if err != nil {
			log.Fatal(err)
		}
		defer reader.Close()

		// используем буфер, чтобы не читать весь большой файл в память
		r := bufio.NewReader(reader)
		// создаем примитив синхронизации
		wg := sync.WaitGroup{}
		// cоздаем канал
		c := make(chan []ClientItem)
		for {
			buf := make([]byte, 4*1024)
			// читаем из буфера небольшими чанками
			n, err := r.Read(buf)
			buf = buf[:n]
			if n == 0 {
				if err == io.EOF {
					break
				}
				if err != nil {
					fmt.Println(err)
					break
				}

			}
			// читаем до конца строки
			nextUntilNewline, err := r.ReadBytes('\n')
			if err != io.EOF {
				buf = append(buf, nextUntilNewline...)
			}
			wg.Add(1)
			// стартуем парсинг чанка в отдельной горутине
			go func() {
				ProcessChunk(buf, c, clients)
				wg.Done()
			}()
			s := <-c
			result = append(result, s...)
		}
		// ждем пока все горутины не завершаться
		wg.Wait()

	}
	fmt.Println(len(result))
	for _, item := range result {
		if err := SetCache(item); err != nil {
			fmt.Println(err)
		}
	}
	log.Printf("completed in %s", time.Since(start))
}

type ClientItem struct {
	item   memcache.Item
	client *memcache.Client
}

func ProcessChunk(chunk []byte, results chan []ClientItem, clients map[string]*memcache.Client) {
	// приводим к строке
	logs := string(chunk)
	// разбиваем на несколько строк
	logsSlice := strings.Split(logs, "\n")

	var items []ClientItem
	for _, logString := range logsSlice {
		if len(logString) == 0 {
			continue
		}
		// разбиваем каждую строку по знаку табуляции
		logSlice := strings.Split(logString, "\t")

		var apps []uint32
		parts := strings.Split(logSlice[4], ",")
		// собираем список apps
		for _, a := range parts {
			a = strings.TrimSpace(a)
			digit, err := strconv.Atoi(a)
			if err != nil {
				fmt.Println(err)
				continue
			}
			apps = append(apps, uint32(digit))
		}

		// парсим значения широты и долготы
		lat, err := strconv.ParseFloat(logSlice[2], 8)
		if err != nil {
			fmt.Println(err)
			continue
		}
		lon, err := strconv.ParseFloat(logSlice[3], 8)
		if err != nil {
			fmt.Println(err)
			continue
		}
		// создаем структуру AppsInstalled
		s := AppsInstalled{
			logSlice[0],
			logSlice[1],
			lat,
			lon,
			apps,
		}

		// создаем сообщение
		m := createMessage(&s)
		// добавляем в список объект ClientItem
		items = append(items, ClientItem{item: m, client: clients[s.devType]})
	}
	results <- items
}

func createMessage(app *AppsInstalled) memcache.Item {
	ua := &appsinstalled.UserApps{
		Lat:  &app.lat,
		Lon:  &app.lon,
		Apps: app.apps,
	}

	key := fmt.Sprintf("%s:%s", app.devType, app.devId)
	packed, _ := proto.Marshal(ua)
	return memcache.Item{
		Key:   key,
		Value: packed,
	}
}

func SetCache(value ClientItem) error {
	cacheErr := value.client.Set(&value.item)
	if cacheErr != nil {
		return cacheErr
	}
	return nil
}
