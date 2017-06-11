const _ = require('lodash');
const Poloniex = require('poloniex-api-node');
const Promise = require('bluebird');
const datastore = require('@google-cloud/datastore')({ promise: Promise });

const poloniexApiDataStoreKey = datastore.key(['poloniex_api', 'ticker']);
const tickerDataStoreKey = datastore.key(['ticker']);

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event The Cloud Functions event.
 * @param {!Function} The callback function.
 */
exports.update = (event, callback) => {
  datastore.get(poloniexApiDataStoreKey).then(function(entity) {
    const poloniexApiKey = entity['API_KEY'];
    const poloniexApiSecret = entity['SECRET'];
    const poloniexClient = new Poloniex(poloniexApiKey, poloniexApiSecret);
    const dateTime = new Date();

    poloniexClient.returnTicker(function(err, tickerData) {
      if (!err) {
        let currencyDataPromises = [];
        _.forOwn(tickerData, function(data, currencyPair) {
          currencyDataPromises.push(
            getCurrencyDataPromise(currencyPair, data, dateTime)
          );
        });

        Promise.all(currencyDataPromises).then(function() {
          callback();
        }).catch(callback);
      } else {
        callback(err);
      }
    });
  }).catch(callback);
};

const getCurrencyDataPromise = (currencyPair, data, dateTime) => {
  const currencyData = {
    currencyPair: currencyPair,
    dateTime: dateTime,
    last: parseFloat(data.last),
    lowestAsk: parseFloat(data.lowestAsk),
    highestBid: parseFloat(data.highestBid),
    percentChange: parseFloat(data.percentChange),
    baseVolume: parseFloat(data.baseVolume),
    quoteVolume: parseFloat(data.quoteVolume),
    isFrozen: data.isFrozen === '1',
    high24hr: parseFloat(data.high24hr)
  };

  return datastore.save({
    key: tickerDataStoreKey,
    data: currencyData
  });
}
