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
  const batchRequests = [getCurrencyData(), getAccountInfo()];
  
 Promise.all(backRequests).then(([currencyDataEntities, accountInfo]) => {
    chooseToBuyOrSell(currencyDataEntities[0], accountInfo).then(() => {
      callback();
    }).catch(callback);
  }).catch(callback);
};

const chooseToBuyOrSell = (currencyData, accountInfo) => {
  if(accountInfo.lastAction == 'SELL') {
    return buy(currencyData, accountInfo);
  } else {
    return sell(currencyData, accountInfo);
  }
}


const getAccountInfo = () => {
  // if no current transcations
  
}

const buy = (currencyData, accountInfo) => {
 currencyData = _.sortBy(currencyData, () => {
   return currencyData.slope * currencyData.volatilityFactor;
 });
  currencyData.reverse();
  
  // sort by splope * volatility
  // make buy decisions
}

const sell = (currencyData, accountInfo) => {
  
}

const broadcastNewTransactionChange = (currency, price, quantity) => {
  const transcationString = currency + ':' + price + ':' + quantity;
  return newTransactionBroadcastTopic.publish(transactionString);
}

const getCurrencyData = () => {
  const query = datastore.createQuery('currency-data');
  return datastore.runQuery(query);
}
