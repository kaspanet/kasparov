package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/http"

	"github.com/kaspanet/kaspad/ecc"
	"github.com/kaspanet/kaspad/txscript"
	"github.com/kaspanet/kaspad/util"
	"github.com/kaspanet/kaspad/util/daghash"
	"github.com/kaspanet/kaspad/wire"
	"github.com/kaspanet/kasparov/kasparovd/apimodels"
	"github.com/pkg/errors"
)

const feeSompis uint64 = 1000

func send(conf *sendConfig) error {
	toAddress, err := util.DecodeAddress(conf.ToAddress, util.Bech32PrefixUnknown)
	if err != nil {
		return err
	}

	privateKey, publicKey, err := parsePrivateKey(conf.PrivateKey)
	if err != nil {
		return err
	}

	fromAddress, err := util.NewAddressPubKeyHashFromPublicKey(publicKey.SerializeCompressed(), toAddress.Prefix())
	if err != nil {
		return err
	}

	utxos, err := getUTXOs(conf.KasparovAddress, fromAddress.String())
	if err != nil {
		return err
	}

	sendAmountSompi := uint64(conf.SendAmount * util.SompiPerKaspa)
	totalToSend := sendAmountSompi + feeSompis

	selectedUTXOs, change, err := selectUTXOs(utxos, totalToSend)
	if err != nil {
		return err
	}

	msgTx, err := generateTx(privateKey, selectedUTXOs, sendAmountSompi, change, toAddress, fromAddress)
	if err != nil {
		return err
	}

	err = sendTx(conf, msgTx)
	if err != nil {
		return err
	}

	fmt.Println("Transaction was sent successfully")
	fmt.Printf("Transaction ID: \t%s", msgTx.TxID())

	return nil
}

func parsePrivateKey(privateKeyHex string) (*ecc.PrivateKey, *ecc.PublicKey, error) {
	privateKeyBytes, err := hex.DecodeString(privateKeyHex)
	if err != nil {
		return nil, nil, errors.Wrap(err, "Error parsing private key hex")
	}
	privateKey, publicKey := ecc.PrivKeyFromBytes(ecc.S256(), privateKeyBytes)
	return privateKey, publicKey, nil
}

func selectUTXOs(utxos []*apimodels.TransactionOutputResponse, totalToSpend uint64) (
	selectedUTXOs []*apimodels.TransactionOutputResponse, change uint64, err error) {

	selectedUTXOs = []*apimodels.TransactionOutputResponse{}
	totalValue := uint64(0)

	for _, utxo := range utxos {
		if utxo.IsSpendable == nil || !*utxo.IsSpendable {
			continue
		}

		selectedUTXOs = append(selectedUTXOs, utxo)
		totalValue += utxo.Value

		if totalValue >= totalToSpend {
			break
		}
	}

	if totalValue < totalToSpend {
		return nil, 0, errors.Errorf("Insufficient funds for send: %f required, while only %f available",
			float64(totalToSpend)/util.SompiPerKaspa, float64(totalValue)/util.SompiPerKaspa)
	}

	return selectedUTXOs, totalValue - totalToSpend, nil
}

func generateTx(privateKey *ecc.PrivateKey, selectedUTXOs []*apimodels.TransactionOutputResponse, sompisToSend uint64, change uint64,
	toAddress util.Address, fromAddress util.Address) (*wire.MsgTx, error) {

	txIns := make([]*wire.TxIn, len(selectedUTXOs))
	for i, utxo := range selectedUTXOs {
		txID, err := daghash.NewTxIDFromStr(utxo.TransactionID)
		if err != nil {
			return nil, err
		}

		txIns[i] = wire.NewTxIn(wire.NewOutpoint(txID, utxo.Index), []byte{})
	}

	toScript, err := txscript.PayToAddrScript(toAddress)
	if err != nil {
		return nil, err
	}
	mainTxOut := wire.NewTxOut(sompisToSend, toScript)

	fromScript, err := txscript.PayToAddrScript(fromAddress)
	if err != nil {
		return nil, err
	}
	changeTxOut := wire.NewTxOut(change, fromScript)

	txOuts := []*wire.TxOut{mainTxOut, changeTxOut}

	tx := wire.NewNativeMsgTx(wire.TxVersion, txIns, txOuts)

	for i, txIn := range tx.TxIn {
		signatureScript, err := txscript.SignatureScript(tx, i, fromScript, txscript.SigHashAll, privateKey, true)
		if err != nil {
			return nil, err
		}
		txIn.SignatureScript = signatureScript
	}

	return tx, nil
}

func sendTx(conf *sendConfig, msgTx *wire.MsgTx) error {
	txBuffer := bytes.NewBuffer(make([]byte, 0, msgTx.SerializeSize()))
	if err := msgTx.KaspaEncode(txBuffer, 0); err != nil {
		return err
	}

	txHex := make([]byte, hex.EncodedLen(txBuffer.Len()))
	hex.Encode(txHex, txBuffer.Bytes())

	rawTx := &apimodels.RawTransaction{
		RawTransaction: string(txHex),
	}
	txBytes, err := json.Marshal(rawTx)
	if err != nil {
		return errors.Wrap(err, "Error marshalling transaction to json")
	}

	response, err := http.Post(fmt.Sprintf("%s/transaction", conf.KasparovAddress), "application/json", bytes.NewBuffer(txBytes))
	if err != nil {
		return errors.Wrap(err, "Error posting transaction to server")
	}
	_, err = readResponse(response)
	if err != nil {
		return errors.Wrap(err, "Error reading send transaction response")
	}

	return err
}
