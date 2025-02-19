package main

import (
	"context"
	"crypto/x509"
	"encoding/json"
	"log"
	"net/url"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/mr-tron/base58"
	"github.com/pkg/errors"
	pb "github.com/rpcpool/yellowstone-grpc/examples/golang/proto"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/keepalive"
	"google.golang.org/grpc/metadata"
)

const MaxMessageSize = 128 * 1024 * 1024 // 128 MB

type TransactionHandler interface {
	HandleTransaction(context.Context, *pb.SubscribeUpdateTransaction) error
}

type BlockHandler interface {
	HandleBlock(context.Context, *pb.SubscribeUpdateBlock) error
}

type SlotHandler interface {
	HandleSlot(context.Context, *pb.SubscribeUpdateSlot) error
}

type SubscriptionTarget int

const (
	Transaction SubscriptionTarget = iota
	Block
	Slot
)

type Client struct {
	endpoint           string
	apiToken           string
	subscriptionTarget SubscriptionTarget
	transactionHandler TransactionHandler
	blockHandler       BlockHandler
	slotHandler        SlotHandler
}

type Config struct {
	Endpoint string
	APIToken string
}

type NewClientParams struct {
	Config             Config
	SubscriptionTarget SubscriptionTarget
	TransactionHandler TransactionHandler
	BlockHandler       BlockHandler
	SlotHandler        SlotHandler
}

func NewClient(params NewClientParams) *Client {
	// Use the endpoint from params.Config instead of hardcoding it
	endpoint := params.Config.Endpoint
	if !strings.HasPrefix(endpoint, "http://") && !strings.HasPrefix(endpoint, "https://") {
		endpoint = "https://" + endpoint
	}

	return &Client{
		endpoint:           endpoint,
		apiToken:           params.Config.APIToken,
		transactionHandler: params.TransactionHandler,
		blockHandler:       params.BlockHandler,
		slotHandler:        params.SlotHandler,
		subscriptionTarget: params.SubscriptionTarget,
	}
}

func (c *Client) Stream(ctx context.Context, fromSlot uint64) error {
	if c.endpoint == "" {
		return errors.New("GRPC endpoint is required")
	}

	u, err := url.Parse(c.endpoint)
	if err != nil {
		return errors.Wrapf(err, "unable to parse url %s", c.endpoint)
	}

	// Infer insecure connection if http is given
	useTLS := true
	if u.Scheme == "http" {
		useTLS = false
	}

	port := u.Port()
	if port == "" {
		if useTLS {
			port = "443"
		} else {
			port = "80"
		}
	}
	hostname := u.Hostname()
	if hostname == "" {
		return errors.New("url must be provided")
	}

	address := hostname + ":" + port

	conn := c.grpcConnect(address, useTLS)

	err = c.grpcSubscribeFrom(ctx, conn, fromSlot)
	if err != nil {
		return errors.Wrap(err, "subscribe failed")
	}

	return conn.Close()
}

func (c *Client) grpcConnect(address string, useTLS bool) *grpc.ClientConn {
	var opts []grpc.DialOption
	if useTLS {
		pool, _ := x509.SystemCertPool()
		creds := credentials.NewClientTLSFromCert(pool, "")
		opts = append(opts, grpc.WithTransportCredentials(creds))
	} else {
		opts = append(opts, grpc.WithTransportCredentials(insecure.NewCredentials()))
	}

	kacp := keepalive.ClientParameters{
		Time:                10 * time.Second, // send pings every 10 seconds if there is no activity
		Timeout:             10 * time.Second, // wait 1 second for ping ack before considering the connection dead
		PermitWithoutStream: true,             // send pings even without active streams
	}
	opts = append(opts, grpc.WithKeepaliveParams(kacp))
	opts = append(opts, grpc.WithDefaultCallOptions(grpc.MaxCallRecvMsgSize(MaxMessageSize)))

	log.Println("Starting grpc client, connecting to", address)
	conn, err := grpc.NewClient(address, opts...)
	if err != nil {
		log.Fatalf("fail to dial: %v", err)
	}

	return conn
}

func (c *Client) grpcSubscribeFrom(ctx context.Context, conn *grpc.ClientConn, fromSlot uint64) error {
	_ = fromSlot // until we support fromSlot, suppress unparam lint
	var err error
	client := pb.NewGeyserClient(conn)

	commitment := pb.CommitmentLevel_CONFIRMED
	subscription := pb.SubscribeRequest{
		Commitment: &commitment,
	}

	blockChan := make(chan *pb.SubscribeUpdateBlock, 50)
	// defer close(blockChan)

	switch c.subscriptionTarget {
	case Transaction:
		fetchFailedTxs := false
		fetchVoteTxs := false

		subscription.Transactions = make(map[string]*pb.SubscribeRequestFilterTransactions)
		subscription.Transactions["transactions_sub"] = &pb.SubscribeRequestFilterTransactions{
			Vote:            &fetchVoteTxs,
			Failed:          &fetchFailedTxs,
			AccountInclude:  []string{"TokenkegQfeZyiNwAJbNbGKPFXCWuBvf9Ss623VQ5DA"},
			AccountExclude:  []string{},
			AccountRequired: []string{},
		}

	case Block:
		go c.publishBlocks(ctx, blockChan)
		includeTransactions := true
		subscription.Blocks = make(map[string]*pb.SubscribeRequestFilterBlocks)
		subscription.Blocks["blocks_sub"] = &pb.SubscribeRequestFilterBlocks{
			IncludeTransactions: &includeTransactions,
			AccountInclude:      []string{},
		}
	case Slot:
		filterByCommitment := true
		subscription.Slots = make(map[string]*pb.SubscribeRequestFilterSlots)
		subscription.Slots["slots_sub"] = &pb.SubscribeRequestFilterSlots{
			FilterByCommitment: &filterByCommitment,
		}
	}

	subscriptionJson, err := json.Marshal(&subscription)
	if err != nil {
		return errors.Wrapf(err, "failed to marshal subscription request: %v", subscriptionJson)
	}

	// Set up the subscription request
	if c.apiToken != "" {
		md := metadata.New(map[string]string{"x-token": c.apiToken})
		ctx = metadata.NewOutgoingContext(ctx, md)
	}

	stream, err := client.Subscribe(ctx)
	if err != nil {
		log.Fatalf("%v", err)
	}
	err = stream.Send(&subscription)
	if err != nil {
		log.Fatalf("%v", err)
	}

	for {
		resp, err := stream.Recv()
		if err != nil {
			return errors.Wrap(err, "error occurred in receiving update")
		}

		switch resp.GetUpdateOneof().(type) {
		case *pb.SubscribeUpdate_Transaction:
			err = c.transactionHandler.HandleTransaction(ctx, resp.GetTransaction())
			if err != nil {
				return errors.Wrap(err, "error occurred in handling the transaction")
			}
		case *pb.SubscribeUpdate_Block:
			blockChan <- resp.GetBlock()
		case *pb.SubscribeUpdate_Slot:
			err := c.slotHandler.HandleSlot(ctx, resp.GetSlot())
			if err != nil {
				return errors.Wrapf(err, "error occurred in handling the slot %d", resp.GetSlot().GetSlot())
			}
		}
	}
}

func (c *Client) publishBlocks(ctx context.Context, blockChan chan *pb.SubscribeUpdateBlock) {
	var wg sync.WaitGroup

	for i := 0; i < 5; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for {
				block := <-blockChan
				err := c.blockHandler.HandleBlock(ctx, block)
				if err != nil {
					log.Fatalf("error occurred in handling the block: %v", err)
				}
			}
		}()
	}
	wg.Wait()
}

func main() {
	cfg := Config{
		Endpoint: "commutator-nix-fheqztyhpv-dedicated-lb.helius-rpc.com:2053",
		APIToken: "Your API Token Here",
	}

	// Create handlers (implement these based on your needs)
	txHandler := &YourTransactionHandler{}
	blockHandler := &YourBlockHandler{}
	slotHandler := &YourSlotHandler{}

	client := NewClient(NewClientParams{
		Config:             cfg,
		SubscriptionTarget: Transaction, // or Block or Slot
		TransactionHandler: txHandler,
		BlockHandler:       blockHandler,
		SlotHandler:        slotHandler,
	})

	ctx := context.Background()
	if err := client.Stream(ctx, 0); err != nil {
		log.Fatal(err)
	}

	// Keep the program running
	select {}
}

// Example handler implementations with file output
type YourTransactionHandler struct{}
type YourBlockHandler struct{}
type YourSlotHandler struct{}

func (h *YourTransactionHandler) HandleTransaction(ctx context.Context, tx *pb.SubscribeUpdateTransaction) error {
	if tx == nil || tx.GetTransaction() == nil {
		return nil
	}

	txInfo := tx.GetTransaction()
	meta := txInfo.GetMeta()
	message := txInfo.GetTransaction().GetMessage()

	txLog := struct {
		// Basic Transaction Info
		Signature string    `json:"signature"`
		Slot      uint64    `json:"slot"`
		Timestamp time.Time `json:"timestamp"`

		// Status and Fees
		Success bool   `json:"success"`
		Fee     uint64 `json:"fee"`

		// Transaction Details
		RecentBlockhash string `json:"recentBlockhash"`

		// Account Information
		AccountKeys []string `json:"accountKeys"`

		// Balance Changes
		PreBalances  []uint64 `json:"preBalances"`
		PostBalances []uint64 `json:"postBalances"`

		// Token Balance Changes
		PreTokenBalances  []TokenBalance `json:"preTokenBalances"`
		PostTokenBalances []TokenBalance `json:"postTokenBalances"`

		// Compute Units
		ComputeUnitsConsumed uint64 `json:"computeUnitsConsumed,omitempty"`

		// Logs
		LogMessages []string `json:"logMessages,omitempty"`

		// Inner Instructions
		InnerInstructions []InnerInstruction `json:"innerInstructions,omitempty"`
	}{
		Signature: base58.Encode(txInfo.GetSignature()),
		Slot:      tx.GetSlot(),
		Timestamp: time.Now().UTC(),
		Success:   meta.GetErr() == nil,
		Fee:       meta.GetFee(),

		RecentBlockhash: base58.Encode(message.GetRecentBlockhash()),
		LogMessages:     meta.GetLogMessages(),

		PreBalances:  meta.GetPreBalances(),
		PostBalances: meta.GetPostBalances(),

		ComputeUnitsConsumed: meta.GetComputeUnitsConsumed(),
	}

	// Convert account keys
	for _, key := range message.GetAccountKeys() {
		txLog.AccountKeys = append(txLog.AccountKeys, base58.Encode(key))
	}

	// Process token balances
	for _, balance := range meta.GetPreTokenBalances() {
		txLog.PreTokenBalances = append(txLog.PreTokenBalances, TokenBalance{
			AccountIndex: balance.GetAccountIndex(),
			Mint:         balance.GetMint(),
			Owner:        balance.GetOwner(),
			Amount:       balance.GetUiTokenAmount().GetAmount(),
			Decimals:     balance.GetUiTokenAmount().GetDecimals(),
		})
	}

	for _, balance := range meta.GetPostTokenBalances() {
		txLog.PostTokenBalances = append(txLog.PostTokenBalances, TokenBalance{
			AccountIndex: balance.GetAccountIndex(),
			Mint:         balance.GetMint(),
			Owner:        balance.GetOwner(),
			Amount:       balance.GetUiTokenAmount().GetAmount(),
			Decimals:     balance.GetUiTokenAmount().GetDecimals(),
		})
	}

	// Process inner instructions
	accountKeys := message.GetAccountKeys()
	for _, inner := range meta.GetInnerInstructions() {
		innerInst := InnerInstruction{
			Index: inner.GetIndex(),
		}
		for _, inst := range inner.GetInstructions() {
			programIdIndex := inst.GetProgramIdIndex()
			if int(programIdIndex) >= len(accountKeys) {
				continue
			}

			instruction := Instruction{
				ProgramID: base58.Encode(accountKeys[programIdIndex]),
				Data:      base58.Encode(inst.GetData()),
			}

			for _, acc := range inst.GetAccounts() {
				if int(acc) >= len(accountKeys) {
					continue
				}
				instruction.Accounts = append(instruction.Accounts, base58.Encode(accountKeys[acc]))
			}
			innerInst.Instructions = append(innerInst.Instructions, instruction)
		}
		txLog.InnerInstructions = append(txLog.InnerInstructions, innerInst)
	}

	jsonData, err := json.MarshalIndent(txLog, "", "  ")
	if err != nil {
		return err
	}

	f, err := os.OpenFile("transaction_logs_geyser.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.WriteString(string(jsonData) + "\n"); err != nil {
		return err
	}
	return nil
}

// Supporting types
type TokenBalance struct {
	AccountIndex uint32 `json:"accountIndex"`
	Mint         string `json:"mint"`
	Owner        string `json:"owner"`
	Amount       string `json:"amount"`
	Decimals     uint32 `json:"decimals"`
}

type Instruction struct {
	ProgramID string   `json:"programId"`
	Data      string   `json:"data"`
	Accounts  []string `json:"accounts"`
}

type InnerInstruction struct {
	Index        uint32        `json:"index"`
	Instructions []Instruction `json:"instructions"`
}

func (h *YourBlockHandler) HandleBlock(ctx context.Context, block *pb.SubscribeUpdateBlock) error {
	// Define reward type struct
	type Reward struct {
		Pubkey      string `json:"pubkey"`
		Lamports    int64  `json:"lamports"`
		PostBalance uint64 `json:"postBalance"`
		RewardType  string `json:"rewardType"`
		Commission  *uint8 `json:"commission,omitempty"`
	}

	// Define transaction metadata struct
	type TransactionMeta struct {
		Err               interface{}        `json:"err"`
		Fee               uint64             `json:"fee"`
		InnerInstructions []InnerInstruction `json:"innerInstructions"`
		LogMessages       []string           `json:"logMessages"`
		PostBalances      []uint64           `json:"postBalances"`
		PostTokenBalances []TokenBalance     `json:"postTokenBalances"`
		PreBalances       []uint64           `json:"preBalances"`
		PreTokenBalances  []TokenBalance     `json:"preTokenBalances"`
		Status            string             `json:"status"`
	}

	// Define transaction struct
	type Transaction struct {
		Meta        TransactionMeta `json:"meta"`
		Transaction struct {
			Message struct {
				AccountKeys     []string      `json:"accountKeys"`
				Instructions    []Instruction `json:"instructions"`
				RecentBlockhash string        `json:"recentBlockhash"`
			} `json:"message"`
			Signatures []string `json:"signatures"`
			Version    *uint8   `json:"version,omitempty"`
		} `json:"transaction"`
	}

	// Define block log struct type
	type BlockLog struct {
		BlockHeight       uint64        `json:"blockHeight"`
		BlockTime         *int64        `json:"blockTime,omitempty"`
		Blockhash         string        `json:"blockhash"`
		ParentSlot        uint64        `json:"parentSlot"`
		PreviousBlockhash string        `json:"previousBlockhash"`
		Transactions      []Transaction `json:"transactions,omitempty"`
		Rewards           []Reward      `json:"rewards,omitempty"`
		ClockSysvar       interface{}   `json:"clockSysvar,omitempty"`
		SlotHashesSysvar  interface{}   `json:"slotHashesSysvar,omitempty"`
		VoteAccount       string        `json:"voteAccount,omitempty"`
		VoteAuthority     string        `json:"voteAuthority,omitempty"`
		BlockType         string        `json:"type,omitempty"`
	}

	// Create and populate the block log
	log := BlockLog{}

	jsonData, err := json.MarshalIndent(log, "", "  ")
	if err != nil {
		return err
	}

	f, err := os.OpenFile("block_logs_geyser.json", os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer f.Close()

	if _, err := f.WriteString(string(jsonData) + "\n"); err != nil {
		return err
	}
	return nil
}

func (h *YourSlotHandler) HandleSlot(ctx context.Context, slot *pb.SubscribeUpdateSlot) error {
	// Implement slot handling logic here
	return nil
}
