const _ = require('lodash');
const Promise = require('bluebird');
const stats = require('simple-statistics');
const datastore = require('@google-cloud/datastore')({ promise: Promise });
const pubsub = require('@google-cloud/pubsub')({ promise: Promise });

const newTransactionBroadcastTopic = pubsub.topic('new-transaction');

const MS_PER_MINUTE = 60000;

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event The Cloud Functions event.
 * @param {!Function} The callback function.
 */
exports.buyOrSell = (event, callback) => {
  getAccountInfo().then((accountInfo) => {
    chooseToBuyOrSell(accountInfo).then(() => {
      callback();
    }).catch(callback);
  }).catch(callback);
};

const chooseToBuyOrSell = (accountInfo) => {
  if(accountInfo.lastAction == 'SELL') {
    return buy(accountInfo);
  } else {
    return sell(accountInfo);
  }
}

const getAccountInfo = () => {
  // if no current transcations
  
}

const buy = (accountInfo) => {
  
}

const sell = (accountInfo) => {

}

const broadcastNewTransactionChange = (currency, price, quantity) => {
  const transcationString = currency + ':' + price + ':' + quantity;
  return currencyChangeBroadcastTopic.publish(transactionString);
}

const getCurrencyData = () => {
  const query = datastore.createQuery('currency-data');
  return datastore.runQuery(query);
}
