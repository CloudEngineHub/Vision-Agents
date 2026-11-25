//go:build integration

package api

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone"
	"github.com/GetStream/Vision-Agents/acceleration/internal/phone/outcome"
	"github.com/GetStream/Vision-Agents/acceleration/internal/store"
)

// CrossShardSessionSuite covers a session watched from a node in another shard, whose relay
// the session's events never cross.
type CrossShardSessionSuite struct {
	RouterSuite

	// other is the node in the other shard, running every session a test here opens.
	other *httptest.Server
}

func TestCrossShardSessionSuite(t *testing.T) {
	runSuite(t, new(CrossShardSessionSuite))
}

func (s *CrossShardSessionSuite) SetupSuite() {
	s.shard, s.otherShard = "here", "there"
	s.RouterSuite.SetupSuite()
	s.other = s.otherNode()
}

func (s *CrossShardSessionSuite) SetupTest() {
	s.useApp(s.data.createApp())
}

func (s *CrossShardSessionSuite) TestASessionInAnotherShardIsWatchedThroughTheNodeRunningIt() {
	answer := s.utils.uuid()
	opened := s.serverClient.on(s.other).createSession(CreateSessionRequest{
		Incognito: pointerTo(true),
		Llm:       pointerTo("echo/echo-model"), ConfigId: s.data.instructedAgent(answer),
	})
	watching := s.serverClient.opens("/v1/agents/sessions/" + opened.Id + "/events")

	s.Require().Equal(http.StatusNoContent, s.serverClient.do(http.MethodPost,
		"/v1/agents/sessions/"+opened.Id+"/respond", SayRequest{Text: "hello"}, nil))

	answered := s.await(watching, "responded")
	s.Contains(answered["text"], answer, "the watcher in this shard never saw the answer")
}

// endedThere is a chat the node in the other shard opened and ended, whose rows are there.
func (s *CrossShardSessionSuite) endedThere() string {
	opened := s.serverClient.on(s.other).createSession(textSession(nil))
	s.serverClient.on(s.other).stopSession(opened.Id)
	ctx := context.Background()
	s.Require().Eventually(func() bool {
		row, err := s.store.Sessions.Get(ctx, s.customerID(), opened.Id)
		home, homeErr := s.store.SessionHomes.Of(ctx, s.customerID(), opened.Id)
		return err == nil && row.State == store.SessionClosed && homeErr == nil && home == "there"
	}, settleFor, 10*time.Millisecond, "the session ended in the shard it lives in")
	return opened.Id
}

// runningOn is the address of the node the deployment's register says runs a session.
func (s *CrossShardSessionSuite) runningOn(id string) string {
	directory, err := node.NewDirectory(node.DirectoryOptions{Redis: s.live.Redis(), Address: "observer",
		Prefix: s.relayPrefix, Logger: slog.New(slog.DiscardHandler)})
	s.Require().NoError(err)
	defer directory.Close()
	address, err := directory.Node(context.Background(), s.customerID(), id)
	s.Require().NoError(err)
	return address
}

func (s *CrossShardSessionSuite) TestAChatLivingInAnotherShardIsCarriedOnThere() {
	id := s.endedThere()

	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodPost,
		"/v1/agents/sessions/"+id+"/resume", ResumeSessionRequest{}, nil))

	s.Equal(s.other.Listener.Addr().String(), s.runningOn(id),
		"carried on in a shard that does not have its rows")
}

func (s *CrossShardSessionSuite) TestASessionLivingInAShardNoNodeAnswersIsUnavailable() {
	id := s.endedThere()
	_, err := s.store.GlobalDB().NewUpdate().Model((*store.SessionHome)(nil)).Set("shard = ?", "gone").
		Where("customer_id = ?", s.customerID()).Where("session_id = ?", id).Exec(context.Background())
	s.Require().NoError(err)

	status, payload := s.serverClient.call(http.MethodGet, "/v1/agents/sessions/"+id, nil)

	s.Equal(http.StatusServiceUnavailable, status, "read from a shard that does not have its rows")
	var answered ErrorResponse
	s.Require().NoError(json.Unmarshal(payload, &answered))
	s.Equal(codeShardUnavailable, answered.Error.Code)
}

// CallShardSuite covers what a vendor or Stream says about a call, delivered to a node in a
// shard other than the one the call lives in.
type CallShardSuite struct {
	RouterSuite

	// otherPhone is the phone service of the node in the other shard, over its own store.
	otherPhone *phone.Service
}

func TestCallShardSuite(t *testing.T) {
	runSuite(t, new(CallShardSuite))
}

func (s *CallShardSuite) SetupSuite() {
	s.shard, s.otherShard = "here", "there"
	s.otherShardApart = true
	s.phoneVendors = s.unreachableVendors()
	s.phoneOnChat = true
	s.RouterSuite.SetupSuite()
	service, err := phone.NewService(phone.ServiceOptions{
		Registry: s.phoneVendors,
		Stream:   phone.NewStreamFromClient(s.chat.Client),
		Store:    s.otherStore,
		Now:      s.phoneNow,
		Logger:   slog.New(slog.DiscardHandler),
	})
	s.Require().NoError(err)
	s.otherPhone = service
	s.otherNodeHolding(nil, s.sealer, slog.New(slog.DiscardHandler), func(options *Options) {
		options.Phone = s.otherPhone
	})
}

func (s *CallShardSuite) SetupTest() {
	s.useFixture("standard")
}

func (s *CallShardSuite) TestAStatusForACallAnotherShardPlacedIsTakenThere() {
	ctx := context.Background()
	callID := s.utils.uuid()
	token, err := s.otherPhone.Outcomes().Placed(ctx, outcome.Call{
		Key: outcome.Key{
			CustomerID: s.customerID(), CallType: "default", CallID: callID,
			Local: s.utils.number(), Remote: s.utils.number(),
		},
		Vendor:  "telnyx",
		Reports: true,
	})
	s.Require().NoError(err)

	request, err := http.NewRequest(http.MethodPost, s.server.URL+"/v1/phone/status/"+token,
		strings.NewReader(url.Values{"CallStatus": {"busy"}}.Encode()))
	s.Require().NoError(err)
	request.Header.Set("Content-Type", "application/x-www-form-urlencoded")
	response, err := s.server.Client().Do(request)
	s.Require().NoError(err)
	s.Require().NoError(response.Body.Close())

	s.Equal(http.StatusNoContent, response.StatusCode)
	call, err := s.otherPhone.Outcomes().Read(ctx, s.customerID(), callID)
	s.Require().NoError(err)
	s.Equal(outcome.Busy, call.Outcome)
}

func (s *CallShardSuite) TestAPlanFetchForAShardNoNodeAnswersIsAnsweredForTheVendorToRetry() {
	status, failure := s.unauthenticatedClient.failure(http.MethodGet, "/v1/phone/answer/gone."+s.utils.uuid(), nil)

	s.Equal(http.StatusServiceUnavailable, status)
	s.Contains(failure, errCallShardDown.Message)
}

func (s *CallShardSuite) TestAnArrivingCallPlacedInAShardNoNodeAnswersIsDeliveredAgain() {
	ctx := context.Background()
	options := s.storeOptions
	options.Shard = "gone"
	gone, err := store.OpenShards(options)
	s.Require().NoError(err)
	s.T().Cleanup(func() { s.NoError(gone.Close()) })
	callID := s.utils.uuid()
	s.Require().NoError(gone.CallResources.Insert(ctx, &store.CallResource{
		TrunkID: "trunk-" + callID, RouteID: "route-" + callID, CallType: "default", CallID: callID,
		CustomerID: s.customerID(), Shared: true,
	}))

	body := arrivingCallAt("default:"+callID, time.Now().UTC())
	status, failure := s.deliver("/v1/phone/hooks/stream", body, sign(body, suiteStreamSecret))

	s.Equal(http.StatusServiceUnavailable, status, "a call another shard placed was taken where it landed")
	s.Contains(failure, errCallShardDown.Message)
}

func (s *CallShardSuite) TestTheEndOfACallAnotherShardMadeIsTakenThere() {
	ctx := context.Background()
	callID := s.utils.uuid()
	s.Require().NoError(s.otherStore.CallResources.Insert(ctx, &store.CallResource{
		TrunkID: "trunk-" + callID, RouteID: "route-" + callID, CallType: "default", CallID: callID,
		CustomerID: s.customerID(), Shared: true,
	}))

	s.Require().Equal(http.StatusOK, s.signedly("/v1/phone/hooks/stream", fmt.Sprintf(
		`{"type":"call.session_ended","call_cid":"default:%s","session_id":"session-1","created_at":%q,`+
			`"call":{"cid":"default:%s","id":"%s","type":"default","custom":{}}}`,
		callID, time.Now().UTC().Format(time.RFC3339), callID, callID)))

	left, err := s.otherStore.CallResources.Select(ctx, store.AppScope{Unpinned: true}, "default", callID)
	s.Require().NoError(err)
	s.Empty(left, "the call's end never reached the shard it lives in")
}

// Defect: a call another shard keeps is read here, where it is missing.
func (s *CallShardSuite) TestACallAnotherShardKeepsIsReadThere() {
	ctx := context.Background()
	call := store.Call{ID: s.utils.uuid(), CustomerID: s.customerID(), AgentID: "agent", CallType: "default",
		CallID: s.utils.uuid(), Direction: "inbound", StartedAt: time.Now().UTC().Truncate(time.Millisecond)}
	s.Require().NoError(s.otherStore.Calls.Start(ctx, &call))
	s.Require().NoError(s.otherStore.ShipIndex(ctx, time.Now()))

	var read Call
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/calls/"+call.ID, nil, &read))

	s.Equal(call.CallID, read.CallId)
}

// Defect: a call list reads this shard alone, or lists another shard's call twice or without
// the shard it is kept in.
func (s *CallShardSuite) TestACallListHoldsEveryShardsCalls() {
	ctx := context.Background()
	at := time.Now().UTC().Truncate(time.Millisecond)
	agent := s.utils.uuid()
	here := store.Call{ID: s.utils.uuid(), CustomerID: s.customerID(), AgentID: agent, CallType: "default",
		CallID: s.utils.uuid(), Direction: "inbound", StartedAt: at}
	there := store.Call{ID: s.utils.uuid(), CustomerID: s.customerID(), AgentID: agent, CallType: "default",
		CallID: s.utils.uuid(), Direction: "inbound", StartedAt: at.Add(time.Second)}
	s.Require().NoError(s.store.Calls.Start(ctx, &here))
	s.Require().NoError(s.otherStore.Calls.Start(ctx, &there))
	s.Require().NoError(s.otherStore.ShipIndex(ctx, time.Now()))

	var listed []Call
	s.Require().Equal(http.StatusOK, s.serverClient.do(http.MethodGet, "/v1/agents/calls?agent_id="+agent, nil, &listed))

	regions := map[string]string{}
	for _, each := range listed {
		regions[each.Id] = value(each.Region)
	}
	s.Equal(map[string]string{here.ID: "here", there.ID: "there"}, regions)
	s.Len(listed, 2)
}
