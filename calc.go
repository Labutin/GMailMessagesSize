package main

import (
	"encoding/json"
	"fmt"
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
//	"gopkg.in/mgo.v2/bson"
)

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

}

type Label struct {
	Id string
	Name string
}

// reInitCollectionForLabels creates collection "labels" in "gmail" database
// and creates indexes
func reInitCollectionForLabels(session *mgo.Session) (*mgo.Collection) {
	lc := session.DB("gmail").C("labels")
	err := lc.DropCollection()
	index := mgo.Index{
		Key: [] string{"id"},
		Unique: true,
	}
	err = lc.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Can't create index for 'labels' collection: %v", err)
	}
	return lc
}

// reInitCollectionForMessages creates collection "messages" in "gmail" database
// and creates indexes
func reInitCollectionForMessages(session *mgo.Session) (*mgo.Collection) {
	mc := session.DB("gmail").C("messages1")
	err := mc.DropCollection()
	index := mgo.Index{
		Key: [] string{"id"},
		Unique: true,
	}
	err = mc.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Can't create index for 'labels' collection: %v", err)
	}
	index = mgo.Index {
		Key: [] string{"Processed"},
	}
	err = mc.EnsureIndex(index)
	if err != nil {
		log.Fatalf("Can't create index for 'labels' collection: %v", err)
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

// importMessages get Messages list from GMail and store its in "Messages" collection
// Doesn't collect info about messages (only messages ids)
func importMessages(srv *gmail.Service, session *mgo.Session) {
	user := "me"
	r, err := srv.Users.Messages.List(user).IncludeSpamTrash(true).Do()
	if err != nil {
		log.Fatalf("Unable to retrieve messages. %v", err)
	}
	var messageToInsert = new(Message)
	messageToInsert.Processed = false
	count := 0
	mc := reInitCollectionForMessages(session)
	for (len(r.Messages) > 0) {
		for _, message := range r.Messages {
			messageToInsert.Id = message.Id
			err = mc.Insert(&messageToInsert)
			if err != nil {
				log.Fatalf("Can't insert message: %v", err)
			}
		}
		count += len(r.Messages)
		fmt.Printf("Processed %d messages\n", count)
		if r.NextPageToken == "" {
			break
		}	else {
			r, err = srv.Users.Messages.List(user).IncludeSpamTrash(true).PageToken(r.NextPageToken).Do()
			if err != nil {
				log.Fatalf("Can't list messages: %v", err)
			}
		}
	}
}

// getMongoDBConnection init MongoDB connection
func getMongoDBConnection() (*mgo.Session, error)  {
	session, err := mgo.Dial("10.211.55.5")
	return session, err
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
	session, err := getMongoDBConnection()
	if err != nil {
		panic(err)
	}
	defer session.Close()

	flagImportLabels := flag.Bool("importLabels", false, "Import Labels from GMail")
	flagImportMessages := flag.Bool("importMessages", false, "Import Messages from GMail")
	flag.Parse()
	if *flagImportLabels {
		importLabels(srv, session)
	}
	if *flagImportMessages {
		importMessages(srv, session)
	}
//	fmt.Printf("%s\n", message.Id)
//	rM, err := srv.Users.Messages.Get(user, message.Id).Fields("sizeEstimate").Do()
//	if err == nil {
//		fmt.Printf("%+v\n", rM);
//	}else {
//		log.Fatalf("Unable to retrieve message. %v", err)
//	}
}
