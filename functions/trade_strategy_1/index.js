const _ = require('lodash');
const Promise = require('bluebird');
const stats = require('simple-statistics');
const datastore = require('@google-cloud/datastore')({ promise: Promise });
const pubsub = require('@google-cloud/pubsub')({ promise: Promise });

const poloniexApiDataStoreKey = datastore.key(['poloniex_api', 'strategy1']);
// const newTransactionBroadcastTopic = pubsub.topic('new-transaction');

const MS_PER_MINUTE = 60000;
const MAX_BUY_DIVIDER = 1/3;

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event The Cloud Functions event.
 * @param {!Function} The callback function.
 */
exports.buyOrSell = (event, callback) => {
  const batchRequests = [getPoloniexClient(), getCurrencyData(), getAccountInfo()];
  
 Promise.all(backRequests).then(([poloniexClient, currencyDataEntities, accountInfo]) => {
    chooseToBuyOrSell(poloniexClient, currencyDataEntities[0], accountInfo).then(() => {
      callback();
    }).catch(callback);
  }).catch(callback);
};

const chooseToBuyOrSell = (poloniexClient, currencyData, accountInfo) => {
  if(accountInfo.lastAction == 'SELL') {
    return buy(poloniexClient, currencyData, accountInfo);
  } else {
    return sell(poloniexClient, currencyData, accountInfo);
  }
}

const getPoloniexClient = () => {
  return new Promise((resolve, reject) => {
    datastore.get(poloniexApiDataStoreKey).then((entity) => {
      const poloniexApiKey = entity['API_KEY'];
      const poloniexApiSecret = entity['SECRET'];
      const poloniexClient = new Poloniex(poloniexApiKey, poloniexApiSecret);
      resolve(poloniexClient);
    }).catch(reject);
  });
}

const getAccountInfo = () => {

  
  
}

const buy = (currencyData, accountInfo) => {
 
  // sort currency data w/ positive slope by slope * volatilityFactor
  let filteredCurrencyData = _.filter(currencyData, (datum) => {
    return datum.slope > 0;
  });
  filteredCurrencyData = _.sortBy(filteredCurrencyData, (datum) => {
    return datum.slope * datum.volatilityFactor;
  });
  filteredCurrencyData.reverse();
 
  
  
  
  // make buy decisions
}

const makeBuyDecision = (availableCash, maxBuyAmount, currencyInfo) => {
  // (currentPrice - pastPrice) / pastPrice > 5 - 4 * volatilityFactor
  // maxBuyAmount
  
}

const sell = (currencyData, accountInfo) => {
  
}

const getCurrencyData = () => {
  const query = datastore.createQuery('currency-data');
  return datastore.runQuery(query);
}
