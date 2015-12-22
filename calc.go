package main

import (
	"encoding/json"
	"fmt"
	"time"
	"strconv"
	"flag"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"os"
	"os/user"
	"path/filepath"

	"golang.org/x/net/context"
	"golang.org/x/oauth2"
	"golang.org/x/oauth2/google"
	"google.golang.org/api/gmail/v1"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"
)

const database string = "gmail"
const labelCollection string = "labels"
const messageCollection string = "messages"

var labelsFlag stringslice
// getClient uses a Context and Config to retrieve a Token
// then generate a Client. It returns the generated Client.
func getClient(ctx context.Context, config *oauth2.Config) *http.Client {
	cacheFile, err := tokenCacheFile()
	if err != nil {
		log.Fatalf("Unable to get path to cached credential file. %v", err)
	}
	tok, err := tokenFromFile(cacheFile)
	if err != nil {
		tok = getTokenFromWeb(config)
		saveToken(cacheFile, tok)
	}
	return config.Client(ctx, tok)
}

// getTokenFromWeb uses Config to request a Token.
// It returns the retrieved Token.
func getTokenFromWeb(config *oauth2.Config) *oauth2.Token {
	authURL := config.AuthCodeURL("state-token", oauth2.AccessTypeOffline)
	fmt.Printf("Go to the following link in your browser then type the " +
	"authorization code: \n%v\n", authURL)

	var code string
	if _, err := fmt.Scan(&code); err != nil {
		log.Fatalf("Unable to read authorization code %v", err)
	}

	tok, err := config.Exchange(oauth2.NoContext, code)
	if err != nil {
		log.Fatalf("Unable to retrieve token from web %v", err)
	}
	return tok
}

// tokenCacheFile generates credential file path/filename.
// It returns the generated credential path/filename.
func tokenCacheFile() (string, error) {
	usr, err := user.Current()
	if err != nil {
		return "", err
	}
	tokenCacheDir := filepath.Join(usr.HomeDir, ".credentials")
	os.MkdirAll(tokenCacheDir, 0700)
	return filepath.Join(tokenCacheDir,
		url.QueryEscape("gmail-go-quickstart.json")), err
}

// tokenFromFile retrieves a Token from a given file path.
// It returns the retrieved Token and any read error encountered.
func tokenFromFile(file string) (*oauth2.Token, error) {
	f, err := os.Open(file)
	if err != nil {
		return nil, err
	}
	t := &oauth2.Token{}
	err = json.NewDecoder(f).Decode(t)
	defer f.Close()
	return t, err
}

// saveToken uses a file path to create a file and store the
// token in it.
func saveToken(file string, token *oauth2.Token) {
	fmt.Printf("Saving credential file to: %s\n", file)
	f, err := os.Create(file)
	if err != nil {
		log.Fatalf("Unable to cache oauth token: %v", err)
	}
	defer f.Close()
	json.NewEncoder(f).Encode(token)
}

type Message struct {
	Id string
	LabelIds []string
	Processed bool
	InternalDate time.Time
}

type Label struct {
	Id string
	Name string
}

// reInitCollectionForLabels creates collection "labels" in "gmail" database
// and creates indexes
func reInitCollectionForLabels(session *mgo.Session) (*mgo.Collection) {
	lc := session.DB(database).C(labelCollection)
	err := lc.DropCollection()
	index := mgo.Index{
		Key: [] string{"id"},
		Unique: true,
	}
	err = lc.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Can't create index for '%s' collection: %v", labelCollection, err)
	}
	return lc
}

// reInitCollectionForMessages creates collection "messages" in "gmail" database
// and creates indexes
func reInitCollectionForMessages(session *mgo.Session) (*mgo.Collection) {
	mc := session.DB(database).C(messageCollection)
//	err := mc.DropCollection()
	index := mgo.Index{
		Key: [] string{"id"},
		Unique: true,
	}
	err := mc.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Can't create index for '%s' collection: %v", messageCollection, err)
	}
	index = mgo.Index {
		Key: [] string{"processed"},
	}
	err = mc.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Can't create index for '%s' collection: %v", messageCollection, err)
	}
	index = mgo.Index {
		Key: [] string{"internaldate"},
	}
	err = mc.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Can't create index for '%s' collection: %v", messageCollection, err)
	}
	index = mgo.Index {
		Key: [] string{"labelids"},
	}
	err = mc.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Can't create index for '%s' collection: %v", messageCollection, err)
	}
	return mc
}

// importLabels get Labels from GMail and store its in "labels" collection
func importLabels(srv *gmail.Service, session *mgo.Session) {
	user := "me"
	r, err := srv.Users.Labels.List(user).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve labels. %v", err)
	}

	lc := reInitCollectionForLabels(session)

	if (len(r.Labels) > 0) {
		for _, l := range r.Labels {
			err = lc.Insert(&Label{l.Id, l.Name})
			if err != nil {
				log.Fatal(err)
			}
		}
	}
	fmt.Printf("Imported labels: %d\n", len(r.Labels))
}

// findLastImportedDay finds date of last message and minus 2 days
func findLastImportedDay(session *mgo.Session) (string) {
	mc := session.DB(database).C(messageCollection)
	lastMessage := new (Message)
	err := mc.Find(nil).Sort("-internaldate").One(&lastMessage)
	if err != nil {
		if err.Error() == "not found" {
			lastMessage.InternalDate = time.Date(1900, time.January, 1, 1, 0, 0, 0, time.UTC)
		} else {
			log.Fatalf("%v", err)
		}
	}
	lastDateMinus2Days := lastMessage.InternalDate.Add(time.Duration(-48) * time.Hour)
	lastDateMinus2DaysStr :=strconv.Itoa(
		lastDateMinus2Days.Year()) +
		"/" + strconv.Itoa(int(lastDateMinus2Days.Month())) +
		"/" + strconv.Itoa(lastDateMinus2Days.Day())
	return lastDateMinus2DaysStr
}

// importMessages get Messages list from GMail and store its in "Messages" collection
// Doesn't collect info about messages (only messages ids)
func importMessages(srv *gmail.Service, session *mgo.Session) {
	user := "me"
	importFromDate := findLastImportedDay(session)
	r, err := srv.Users.Messages.List(user).IncludeSpamTrash(true).Q("newer:"+importFromDate).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve messages. %v", err)
	}
	var messageToInsert = new(Message)
	messageToInsert.Processed = false
	messageToInsert.InternalDate = time.Date(1900, time.January, 1, 1, 0, 0, 0, time.UTC)
	count := 0
	mc := reInitCollectionForMessages(session)
	for (len(r.Messages) > 0) {
		for _, message := range r.Messages {
//			fmt.Printf("%+v\n", message);
			messageToInsert.Id = message.Id
			err = mc.Insert(&messageToInsert)
			if err != nil {
				if (!mgo.IsDup(err)) {
					log.Fatalf("Can't insert message: %v", err)
				}
			}
		}
		count += len(r.Messages)
		fmt.Printf("Processed %d messages\n", count)
		if r.NextPageToken == "" {
			break
		}	else {
			r, err = srv.Users.Messages.List(user).IncludeSpamTrash(true).PageToken(r.NextPageToken).Q("newer:"+importFromDate).Do()
			if err != nil {
				log.Fatalf("Can't list messages: %v\n", err)
			}
		}
	}
}

// processMessage Get information about messages gotten from input chan
func processMessage(srv *gmail.Service, session *mgo.Session, in <-chan string) {
	user := "me"
	mCollection := session.DB(database).C(messageCollection)
	for messageId := range in {
		fmt.Print(".")
		message, err := srv.Users.Messages.Get(user, messageId).Fields("internalDate,labelIds,sizeEstimate").Do()
		if err != nil {
			switch err.Error() {
			default:
				fmt.Printf("Retrive message error: %v\n", err)
				return
			case "googleapi: Error 403: User Rate Limit Exceeded, userRateLimitExceeded":
				fmt.Print("S")
				time.Sleep(5 * time.Second)
			case "googleapi: Error 404: Not Found, notFound":
				err = mCollection.Remove(bson.M{"id": messageId})
				fmt.Print("NF")
				if err != nil {
					fmt.Printf("Can't dete message info: %v\n", err)
				}
			}
		} else {
			err = mCollection.Update(bson.M{"id": messageId}, bson.M{"$set":
				bson.M{
					"processed": true,
					"SizeEstimate": message.SizeEstimate,
					"labelids": message.LabelIds,
					"internaldate": time.Unix(message.InternalDate / 1000, 0)}})
			if err != nil {
				log.Fatalf("Can't update message info: %v\n", err)
			}
		}
	}
}

// processMessages Process all messages in queue (processed==false)
func processMessages(srv *gmail.Service, session *mgo.Session, procNum int) {
	if (procNum < 1 || procNum > 50) {
		log.Fatal("Wrong procNum. Min=1 Max=50")
	}
	flagContinue := true
	var messages []Message
	messagesCollection := session.DB(database).C(messageCollection)
	out := make(chan string)
	for i:= 0; i < procNum; i++ {
		go processMessage(srv, session, out)
	}
	count := 0
	for flagContinue {
		err := messagesCollection.Find(bson.M{"processed": false}).Limit(100).All(&messages)
		if err != nil {
			log.Fatalf("Can't get messages for process: %v", err)
		}
		for _, m := range messages {
			out <- m.Id
		}
		count += len(messages)
		if count % 100 == 0 {
			fmt.Printf("Procecced %d messages\n", count)
		}
		if len(messages) == 0 {
			flagContinue = false
		}
	}
}

func showLabelSize(session *mgo.Session, labelIds []string) {
	messagesCollection := session.DB(database).C(messageCollection)
	labelsCollection := session.DB(database).C(labelCollection)
	var labels []Label
	labelsCollection.Find(bson.M{"id": bson.M{"$in": labelIds}}).All(&labels)
	first := true
	for _, l := range labels {
		if !first {
			fmt.Print(",")
		}else{
			first = false
		}
		fmt.Printf("%s", l.Id)
	}
	fmt.Print(";")
	first = true
	for _, l := range labels {
		if !first {
			fmt.Print(",")
		}else{
			first = false
		}
		fmt.Printf("%s", l.Name)
	}
	fmt.Print(";")
	res := bson.M{}
	lIdsQuery := bson.M{}
	if len(labelIds) == 1 {
		lIdsQuery = bson.M{"$in": labelIds}
	}else {
		lIdsQuery = bson.M{"$all": labelIds, "$size": len(labelIds)}
	}
	err := messagesCollection.Pipe([]bson.M{
		{"$match": bson.M{"labelids": lIdsQuery}},
		{"$group": bson.M{"_id": nil,
			"sum": bson.M{"$sum": "$SizeEstimate"},
			"count": bson.M{"$sum": 1}}}}).One(&res)
	if err != nil {
		if err.Error() == "not found" {
			res["sum"] = 0
			res["count"] = 0
		}else {
			log.Fatalf("Can't calculate Label size: %v", err)
		}
	}
	fmt.Printf("%d;%d\n", res["sum"], res["count"])
}

func showLabelSizes(session *mgo.Session) {
	labelsCollection := session.DB(database).C(labelCollection)
	var labels []Label
	var err error
	err = nil
	if len(labelsFlag) == 0 {
		err = labelsCollection.Find(nil).Sort("name").All(&labels)
	}
	if err != nil {
		log.Fatalf("Can't get Labels list: %v", err)
	}
	fmt.Print("LabelId;Label name;Messages size;Messages count\n")
	if len(labelsFlag) == 0 {
		for _, label := range labels {
			showLabelSize(session, []string{label.Id})
		}
	}else{
		showLabelSize(session, labelsFlag)
	}
}

// getMongoDBConnection init MongoDB connection
func getMongoDBConnection(connectionString string) (*mgo.Session, error)  {
	session, err := mgo.Dial(connectionString)
	return session, err
}

// Define a type named "intslice" as a slice of ints
type stringslice []string

// Now, for our new type, implement the two methods of
// the flag.Value interface...
// The first method is String() string
func (i *stringslice) String() string {
	return fmt.Sprintf("%s", *i)
}

// The second method is Set(value string) error
func (i *stringslice) Set(value string) error {
	*i = append(*i, value)
	return nil
}

func main() {
	ctx := context.Background()

	b, err := ioutil.ReadFile("client_secret.json")
	if err != nil {
		log.Fatalf("Unable to read client secret file: %v", err)
	}

	config, err := google.ConfigFromJSON(b, gmail.GmailReadonlyScope)
	if err != nil {
		log.Fatalf("Unable to parse client secret file to config: %v", err)
	}
	client := getClient(ctx, config)

	srv, err := gmail.New(client)
	if err != nil {
		log.Fatalf("Unable to retrieve gmail Client %v", err)
	}

	flagImportLabels := flag.Bool("importLabels", false, "Import Labels from GMail")
	flagImportMessages := flag.Bool("importMessages", false, "Import Messages from GMail")
	flagProcessMessages := flag.Bool("processMessages", false, "Process Messages (Collect sizes)")
	flagShowSizes := flag.Bool("showSizes", false, "Show Labels sizes")
	procNum := flag.Int("procNum", 1, "Number councurrent processes")
	flagMongoConnectString := flag.String("mongoConnectString", "127.0.0.1", "Mongo connection string")
	flag.Var(&labelsFlag, "l", "List of labels")
	flag.Parse()
	session, err := getMongoDBConnection(*flagMongoConnectString)
	if err != nil {
		log.Fatalf("Can't connect to MongoDB: %v", err)
	}
	defer session.Close()
	if *flagImportLabels {
		importLabels(srv, session)
	}
	if *flagImportMessages {
		importMessages(srv, session)
	}
	if *flagProcessMessages {
		processMessages(srv, session, *procNum)
	}
	if *flagShowSizes {
		showLabelSizes(session)
	}
//	fmt.Printf("%s\n", message.Id)
//	rM, err := srv.Users.Messages.Get(user, message.Id).Fields("sizeEstimate").Do()
//	if err == nil {
//		fmt.Printf("%+v\n", rM);
//	}else {
//		log.Fatalf("Unable to retrieve message. %v", err)
//	}
}
