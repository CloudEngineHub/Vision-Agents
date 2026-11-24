//go:build integration

package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/GetStream/Vision-Agents/acceleration/internal/node"
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
