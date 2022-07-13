package neo4j

// import (
// 	"fmt"
// 	"log"
// 	"testing"

// 	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
// 	"github.com/cloudprivacylabs/lsa/pkg/ls"
// 	"github.com/cloudprivacylabs/opencypher"
// 	"github.com/cloudprivacylabs/opencypher/graph"
// 	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
// 	"github.com/ory/dockertest"
// 	// . "github.com/onsi/ginkgo"
// 	// . "github.com/onsi/gomega"
// )

// // type expectedStruct struct {
// // 	sources []neo4jNode
// // 	targets []neo4jNode
// // 	edges   []neo4jEdge
// // }

// // var expected = []expectedStruct{
// // 	{
// // 		sources: []neo4jNode{
// // 			{id: 60, labels: []string{ls.DocumentNodeTerm}, props: map[string]interface{}{"ls:entitySchema": "Schema for a city"}},
// // 		},
// // 		targets: []neo4jNode{
// // 			{id: 61, labels: []string{ls.DocumentNodeTerm}, props: map[string]interface{}{"value": "San Francisco"}},
// // 			{id: 62, labels: []string{ls.DocumentNodeTerm}, props: map[string]interface{}{"value": "Denver"}},
// // 		},
// // 		edges: []neo4jEdge{
// // 			{id: 28, props: map[string]interface{}{"type": "city"}, startId: 60, endId: 61},
// // 			{id: 29, props: map[string]interface{}{"type": "city"}, startId: 60, endId: 62},
// // 		},
// // 	},
// // 	{
// // 		sources: []neo4jNode{
// // 			{id: 61, labels: []string{ls.DocumentNodeTerm}, props: map[string]interface{}{"value": "San Francisco"}},
// // 			{id: 62, labels: []string{ls.DocumentNodeTerm}, props: map[string]interface{}{"value": "Denver"}},
// // 		},
// // 	},
// // }

// // func getMockFindNeighbor(expected []expectedStruct) func(tx neo4j.Transaction, ids []uint64) ([]neo4jNode, []neo4jNode, []neo4jEdge, error) {
// // 	seq := -1
// // 	return func(tx neo4j.Transaction, ids []uint64) ([]neo4jNode, []neo4jNode, []neo4jEdge, error) {
// // 		seq++
// // 		if seq < len(expected) {
// // 			x := expected[seq]
// // 			return x.sources, x.targets, x.edges, nil
// // 		}
// // 		return nil, nil, nil, nil
// // 	}
// // }

// func selectEntity(node graph.Node) bool {
// 	return true
// }

// type transaction struct{}

// func (transaction) Run(cypher string, params map[string]interface{}) (neo4j.Result, error) {
// 	return nil, nil
// }
// func (transaction) Commit() error   { return nil }
// func (transaction) Rollback() error { return nil }
// func (transaction) Close() error    { return nil }

// func TestLoadEntityNodes(t *testing.T) {
// 	// RegisterFailHandler(Fail)
// 	// RunSpecs(t, "testing ginkgo")
// 	var cfg Config
// 	if err := cmdutil.ReadJSONOrYAML("lsaneo/lsaneo.config.yaml", &cfg); err != nil {
// 		t.Fatalf("Could not read file: %s", "lsaneo/lsaneo.config.yaml")
// 	}
// 	InitNamespaceTrie(&cfg)
// 	tx := transaction{}
// 	roots := make([]uint64, 0)
// 	// for _, node := range expected[0].sources {
// 	// 	roots = append(roots, uint64(node.id))
// 	// }
// 	grph, err := cmdutil.ReadJSONGraph([]string{"examples/test.json"}, nil)
// 	if err != nil {
// 		t.Errorf("Could not read file: %s", "examples/test.json")
// 	}
// 	// var exp []expectedStruct
// 	// uniqueIds := make(map[graph.Node]int64)
// 	// for itr := grph.GetEdges(); itr.Next(); {
// 	// 	estr := expectedStruct{}
// 	// 	edge := itr.Edge()
// 	// 	n4jEdge := neo4jEdge{types: edge.GetLabel()}
// 	// 	if edge.GetFrom() != nil {
// 	// 		src := edge.GetFrom()
// 	// 		if _, ok := uniqueIds[src]; !ok {
// 	// 			uniqueIds[src] = int64(rand.Intn(1000))
// 	// 		}
// 	// 		id := uniqueIds[src]
// 	// 		if err != nil {
// 	// 			t.Errorf("Error parsing node ID, mismatch for %v", id)
// 	// 			t.Fatal()
// 	// 		}
// 	// 		props := make(map[string]interface{})
// 	// 		src.ForEachProperty(func(s string, i interface{}) bool {
// 	// 			props[s] = cfg.MakeProperties(src, map[string]interface{}{})
// 	// 			return true
// 	// 		})
// 	// 		n4jNode := neo4jNode{
// 	// 			id:     id,
// 	// 			labels: []string{cfg.MakeLabels(src.GetLabels().Slice())},
// 	// 			props:  props,
// 	// 		}
// 	// 		estr.sources = append(estr.sources, n4jNode)
// 	// 		n4jEdge.startId = id
// 	// 	}
// 	// 	if edge.GetTo() != nil {
// 	// 		to := edge.GetTo()
// 	// 		if _, ok := uniqueIds[to]; !ok {
// 	// 			uniqueIds[to] = int64(rand.Intn(1000))
// 	// 		}
// 	// 		id := uniqueIds[to]
// 	// 		if err != nil {
// 	// 			t.Errorf("Error parsing node ID, mismatch for %v", id)
// 	// 			t.Fatal()
// 	// 		}
// 	// 		props := make(map[string]interface{})
// 	// 		to.ForEachProperty(func(s string, i interface{}) bool {
// 	// 			props[s] = cfg.MakeProperties(to, map[string]interface{}{})
// 	// 			return true
// 	// 		})
// 	// 		n4jNode := neo4jNode{
// 	// 			id:     id,
// 	// 			labels: []string{cfg.MakeLabels(to.GetLabels().Slice())},
// 	// 			props:  props,
// 	// 		}
// 	// 		estr.targets = append(estr.targets, n4jNode)
// 	// 		n4jEdge.endId = id
// 	// 	}
// 	// 	estr.edges = append(estr.edges, n4jEdge)
// 	// 	exp = append(exp, estr)
// 	// }

// 	fmt.Println("----------------------------------------------------------------------------------------------------------------------------------------------")
// 	// for ix := range exp {
// 	// 	fmt.Println(exp[ix].edges)
// 	// }
// 	// t.Fatal()
// 	fmt.Println()
// 	// gr := ls.NewDocumentGraph(
// 	_, err = loadEntityNodes(tx, grph, roots, cfg, findNeighbors, selectEntity)
// 	ectx := opencypher.NewEvalContext(grph)
// 	v, err := opencypher.ParseAndEvaluate("MATCH (n)-[e]->(m) return n,m,e", ectx)
// 	if err != nil {
// 		t.Error()
// 	}

// 	rs := v.Get().(opencypher.ResultSet)
// 	expectedGraph := ls.NewDocumentGraph()

// 	for _, row := range rs.Rows {
// 		x := row["1"].Get().(graph.Node)
// 		y := row["2"].Get().(graph.Node)
// 		tmp := MakeProperties(ls.CloneProperties(x))
// 		for k, v := range tmp {
// 			x.SetProperty(cfg.Map(k), v)
// 		}
// 		tm := MakeProperties(ls.CloneProperties(y))
// 		for k, v := range tm {
// 			y.SetProperty(cfg.Map(k), v)
// 		}
// 		edge := row["3"].Get().([]graph.Edge)
// 		src := expectedGraph.NewNode([]string{cfg.MakeLabels(x.GetLabels().Slice())}, ls.CloneProperties(x))
// 		target := expectedGraph.NewNode([]string{cfg.MakeLabels(x.GetLabels().Slice())}, ls.CloneProperties(y))
// 		expectedGraph.NewEdge(src, target, edge[0].GetLabel(), ls.CloneProperties(edge[0]))
// 	}

// 	// x := rs.Rows[0]["1"].Get().(graph.Node)
// 	// y := rs.Rows[0]["2"].Get().(graph.Node)
// 	// edge := rs.Rows[0]["3"].Get().([]graph.Edge)
// 	// src := expectedGraph.NewNode([]string{cfg.MakeLabels(x.GetLabels().Slice())}, ls.CloneProperties(x))
// 	// target := expectedGraph.NewNode([]string{cfg.MakeLabels(x.GetLabels().Slice())}, ls.CloneProperties(y))
// 	// expectedGraph.NewEdge(src, target, edge[0].GetLabel(), ls.CloneProperties(edge[0]))

// 	// b := rs.Rows[1]["2"].Get().(graph.Node)
// 	// e := rs.Rows[1]["3"].Get().([]graph.Edge)
// 	// n := expectedGraph.NewNode([]string{cfg.MakeLabels(y.GetLabels().Slice())}, ls.CloneProperties(b))
// 	// expectedGraph.NewEdge(src, n, edge[0].GetLabel(), ls.CloneProperties(e[0]))

// 	gotSources := make([]graph.Node, 0)
// 	expectedSources := make([]graph.Node, 0)
// 	for nodeItr := grph.GetNodes(); nodeItr.Next(); { // 83
// 		gotSources = append(gotSources, nodeItr.Node())
// 	}
// 	for nodeItr := expectedGraph.GetNodes(); nodeItr.Next(); {
// 		expectedSources = append(expectedSources, nodeItr.Node())
// 	}
// 	// vexp := 0
// 	// for ix := range exp {
// 	// 	vexp += len(exp[ix].sources)
// 	// 	vexp += len(exp[ix].targets)
// 	// }

// 	if len(gotSources) != len(expectedSources) {
// 		t.Fatalf("MISMATCH GRAPHS -- gotSources = %d, expectedSources = %d", len(gotSources), len(expectedSources))
// 	}

// 	for g := range gotSources {
// 		matched := false
// 		for e := range expectedSources {
// 			eq := graph.CheckIsomorphism(gotSources[g].GetGraph(), expectedSources[e].GetGraph(), func(n1, n2 graph.Node) bool {
// 				// If only one of the source nodes match, return false
// 				if n1 == gotSources[g] {
// 					if n2 == expectedSources[e] {
// 						return true
// 					}
// 					return false
// 				}
// 				if n2 == expectedSources[e] {
// 					return false
// 				}
// 				// Expected properties must be a subset
// 				propertiesOK := true
// 				n2.ForEachProperty(func(k string, v interface{}) bool {
// 					pv, ok := v.(*ls.PropertyValue)
// 					if !ok {
// 						return true
// 					}
// 					v2, ok := n1.GetProperty(k)
// 					if !ok {
// 						t.Logf("Error at %s: %v: Property does not exist", k, v)
// 						propertiesOK = false
// 						return false
// 					}
// 					pv2, ok := v2.(*ls.PropertyValue)
// 					if !ok {
// 						t.Logf("Error at %s: %v: Not property value", k, v)
// 						propertiesOK = false
// 						return false
// 					}
// 					if !pv2.IsEqual(pv) {
// 						t.Logf("Error at %s: %v: Values are not equal", k, v)
// 						propertiesOK = false
// 						return false
// 					}
// 					return true
// 				})
// 				if !propertiesOK {
// 					t.Logf("Properties not same")
// 					return false
// 				}
// 				t.Logf("True\n")
// 				return true
// 			}, func(e1, e2 graph.Edge) bool {
// 				return ls.IsPropertiesEqual(ls.PropertiesAsMap(e1), ls.PropertiesAsMap(e2))
// 			})
// 			if eq {
// 				matched = true
// 				break
// 			}
// 		}
// 		if !matched {
// 			m := ls.JSONMarshaler{}
// 			result, _ := m.Marshal(grph)
// 			expected, _ := m.Marshal(expectedGraph)
// 			t.Fatalf("Result is different from the expected: Result:\n%s\nExpected:\n%s", string(result), string(expected))
// 		}
// 	}
// }

// /*
// TestContainers plays well with the native go test framework.

// The ideal use case is for integration or end to end tests. It helps you to spin up and manage the dependencies life cycle via Docker.

// Docker has to be available for this library to work.
// */
// // start docker container before testing

// // var _ = Describe("Driver", func() {
// // 	const username = "neo4j"
// // 	const pwd = "password"
// // 	const uri = "neo4j://34.213.163.7"
// // 	const db = "neo4j"

// // 	var ctx context.Context
// // 	var neo4jContainer testcontainers.Container
// // 	var driver neo4j.Driver

// // 	BeforeEach(func() {
// // 		fmt.Println("HERER")
// // 		ctx = context.Background()
// // 		var err error
// // 		neo4jContainer, err = startContainer(ctx, username, pwd)
// // 		Expect(err).To(BeNil(), "Container should start")
// // 		port, err := neo4jContainer.MappedPort(ctx, "7687")
// // 		Expect(err).To(BeNil(), "Port should be resolved")
// // 		address := fmt.Sprintf("%s:%d", uri, port.Int())
// // 		driver, err = neo4j.NewDriver(address, neo4j.BasicAuth(username, pwd, ""))
// // 		Expect(err).To(BeNil(), "Driver should be created")
// // 		// drv := NewDriver(driver, "db")
// // 	})

// // 	AfterEach(func() {
// // 		Close(driver, "Driver")
// // 		Expect(neo4jContainer.Terminate(ctx)).To(BeNil(), "Container should stop")
// // 	})

// // 	It("Add to database", func() {

// // 	})

// // 	It("Load from database", func() {
// // 		// var cfg Config
// // 		// if err := cmdutil.ReadJSONOrYAML("lsaneo/lsaneo.config.yaml", &cfg); err != nil {
// // 		// 	log.Fatalf("Could not read file: %s", "lsaneo/lsaneo.config.yaml")
// // 		// }
// // 		// InitNamespaceTrie(&cfg)
// // 		// tx := transaction{}
// // 		// roots := make([]uint64, 0)
// // 		// // for _, node := range expected[0].sources {
// // 		// // 	roots = append(roots, uint64(node.id))
// // 		// // }
// // 		// grph, err := cmdutil.ReadJSONGraph([]string{"examples/test.json"}, nil)
// // 		// if err != nil {
// // 		// 	log.Fatalf("Could not read file: %s", "examples/test.json")
// // 		// }
// // 	})
// // })

// // func Close(closer io.Closer, resourceName string) {
// // 	Expect(closer.Close()).To(BeNil(), "%s should be closed", resourceName)
// // }

// // func startContainer(ctx context.Context, user, pwd string) (testcontainers.Container, error) {
// // 	request := testcontainers.ContainerRequest{
// // 		Image:        "neo4j",
// // 		ExposedPorts: []string{"7687/tcp"},
// // 		Env:          map[string]string{"NEO4J_AUTH": fmt.Sprintf("%s/%s", user, pwd)},
// // 		WaitingFor:   wait.ForLog("Bolt enabled"),
// // 	}
// // 	return testcontainers.GenericContainer(ctx, testcontainers.GenericContainerRequest{
// // 		ContainerRequest: request,
// // 		Started:          true,
// // 	})
// // }

// func NewAdapter(address, pwd, port, user, dbName string) (neo4j.Driver, error) {
// 	driver, err := neo4j.NewDriver(address, neo4j.BasicAuth(user, pwd, ""))
// 	if err != nil {
// 		return nil, err
// 	}
// 	return driver, nil
// }

// func getAdapter(address, pwd, port, user, dbName string) (neo4j.Driver, error) {
// 	return NewAdapter(address, pwd, port, user, dbName)
// }

// // the test assumes that the Docker engine instance is running on the same machine where the test is being run.
// func setup(user, pwd string) *dockertest.Resource {
// 	pool, err := dockertest.NewPool("")
// 	if err != nil {
// 		log.Fatalf("Could not connect to docker: %s", err)
// 	}
// 	// pull image, creates a container based on it and runs it
// 	resource, err := pool.Run("neo4j", "", []string{fmt.Sprintf("NEO4J_USER=%s", user), fmt.Sprintf("NEO4J_PASSWORD=%s", pwd)})
// 	if err != nil {
// 		log.Fatalf("could not start resource %s", err)
// 	}
// 	var adapter neo4j.Driver
// 	testPort := resource.GetPort("7687/tcp")
// 	if err := pool.Retry(func() error {
// 		adapter, err = getAdapter("neo4j://34.213.163.7:", pwd, testPort, user, "db")
// 		return err
// 	}); err != nil {
// 		log.Fatalf("could not connect to docker: %s", err)
// 	}
// 	initTestAdapter(adapter)
// 	return resource
// }

// // initTestAdapter inserts graph into the database
// func initTestAdapter(adapter neo4j.Driver) {
// 	// testNumbers := []storage.PhoneNumber{
// 	// 	{
// 	// 		ID:     1,
// 	// 		Number: "1234565454",
// 	// 	},
// 	// 	{
// 	// 		ID:     2,
// 	// 		Number: "4565434343",
// 	// 	},
// 	// }
// 	// err := adapter.insertNumbers(testNumbers) // Insert numbers for testing purposes
// 	// if err != nil {
// 	// 	log.Fatalf("error inserting test numbers %v", err)
// 	// }
// }
