package neo4j

import (
	"context"
	"fmt"
	"log"
	"strings"
	"testing"

	"github.com/cloudprivacylabs/lpg"
	"github.com/cloudprivacylabs/lsa/layers/cmd/cmdutil"
	"github.com/cloudprivacylabs/lsa/pkg/ls"
	"github.com/neo4j/neo4j-go-driver/v4/neo4j"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/testcontainers/testcontainers-go"
)

func TestLsaNeo4j(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "LsaNeo4j Suite")
}

func selectEntity(node *lpg.Node) bool {
	return true
}

var _ = Describe("Driver", func() {
	var ctx context.Context
	var neo4jContainer testcontainers.Container
	var session *Session
	var driver neo4j.Driver
	var tx neo4j.Transaction

	var cfg Config
	var grph *lpg.Graph
	var eids []uint64
	var err error

	BeforeEach(func() {
		ctx = context.Background()
		var err error
		neo4jContainer, err = startContainer(ctx, username, pwd)
		Expect(err).To(BeNil(), "Container should start")
		//port, err := neo4jContainer.MappedPort(ctx, "7687")
		Expect(err).To(BeNil(), "Port should be resolved")
		address := fmt.Sprintf("%s:%d", uri, port)
		driver, err = neo4j.NewDriver(address, neo4j.BasicAuth(username, pwd, ""))
		Expect(err).To(BeNil(), "Driver should be created")
		err = cmdutil.ReadJSONOrYAML("lsaneo/lsaneo.config.yaml", &cfg)
		Expect(err).To(BeNil(), "Could not read file: %s", "lsaneo/lsaneo.config.yaml")
		InitNamespaceTrie(&cfg)
		grph, err = cmdutil.ReadJSONGraph([]string{"testdata/test.json"}, nil)
		Expect(err).To(BeNil(), "Could not read file: %s", "testdata/test.json")
	})

	AfterEach(func() {
		Close(driver, "Driver")
		Expect(neo4jContainer.Terminate(ctx)).To(BeNil(), "Container should stop")
	})

	It("Post to database", func() {
		drv := NewDriver(driver, db)
		session = drv.NewSession()
		defer session.Close()
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil(), "must be valid transaction")
		eids, _ = SaveGraph(ls.DefaultContext(), session, tx, grph, selectEntity, cfg, 0)
	})

	It("Load from database", func() {
		drv := NewDriver(driver, db)
		session = drv.NewSession()
		defer session.Close()
		tx, err = session.BeginTransaction()
		Expect(err).To(BeNil(), "must be valid transaction")
		expectedGraph := ls.NewDocumentGraph()
		_, err := loadEntityNodes(tx, expectedGraph, eids, cfg, findNeighbors, selectEntity)
		Expect(err).To(BeNil(), "unable to load nodes connected to entity", err)

		// graph isomorphism
		gotSources := make([]*lpg.Node, 0)
		expectedSources := make([]*lpg.Node, 0)
		edgeSources := make([]*lpg.Edge, 0)
		expectedEdgeSources := make([]*lpg.Edge, 0)
		for nodeItr := grph.GetNodes(); nodeItr.Next(); {
			gotSources = append(gotSources, nodeItr.Node())
		}
		for nodeItr := expectedGraph.GetNodes(); nodeItr.Next(); {
			expectedSources = append(expectedSources, nodeItr.Node())
		}
		for edgeItr := grph.GetEdges(); edgeItr.Next(); {
			edge := edgeItr.Edge()
			edgeSources = append(edgeSources, edge)
		}
		for edgeItr := expectedGraph.GetEdges(); edgeItr.Next(); {
			edge := edgeItr.Edge()
			expectedEdgeSources = append(expectedEdgeSources, edge)
		}

		Expect(len(gotSources)).To(Equal(len(expectedSources)), "mismatch number of nodes")
		Expect(len(edgeSources)).To(Equal(len(expectedEdgeSources)), "mismatch number of edges %d, %d", len(edgeSources), len(expectedEdgeSources))

		for g := range gotSources {
			matched := false
			for e := range expectedSources {
				eq := lpg.CheckIsomorphism(gotSources[g].GetGraph(), expectedSources[e].GetGraph(), func(n1, n2 *lpg.Node) bool {
					if !n1.GetLabels().IsEqual(n2.GetLabels()) {
						return false
					}
					// If only one of the source nodes match, return false
					if n1 == gotSources[g] {
						if n2 == expectedSources[e] {
							return true
						}
						return false
					}
					if n2 == expectedSources[e] {
						return false
					}

					// Expected properties must be a subset
					propertiesOK := true
					n2.ForEachProperty(func(k string, v interface{}) bool {
						pv, ok := v.(*ls.PropertyValue)
						if !ok {
							return true
						}
						v2, ok := n1.GetProperty(k)
						if !ok {
							log.Printf("Error at %s: %v: Property does not exist", k, v)
							propertiesOK = false
							return false
						}
						pv2, ok := v2.(*ls.PropertyValue)
						if !ok {
							log.Printf("Error at %s: %v: Not property value", k, v)
							propertiesOK = false
							return false
						}
						if !pv2.IsEqual(pv) {
							if strings.ToLower(pv2.AsString()) != strings.ToLower(pv.AsString()) {
								log.Printf("Error at %s: Got %v, Expected %v: Values are not equal", k, pv, pv2)
								propertiesOK = false
								return false
							}
						}
						return true
					})
					if !propertiesOK {
						log.Printf("Properties not same")
						return false
					}
					return true
				}, func(e1, e2 *lpg.Edge) bool {
					return e1.GetLabel() == e2.GetLabel() && ls.IsPropertiesEqual(ls.PropertiesAsMap(e1), ls.PropertiesAsMap(e2))
				})
				if eq {
					matched = true
					break
				}
			}
			if !matched {
				m := ls.JSONMarshaler{}
				result, _ := m.Marshal(grph)
				expected, _ := m.Marshal(expectedGraph)
				log.Fatalf("Result is different from the expected: Result:\n%s\nExpected:\n%s", string(result), string(expected))
			}
		}
	})
})
