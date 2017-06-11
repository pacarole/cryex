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
  datastore.get(poloniexApiDataStoreKey).then((entity) => {
    const poloniexApiKey = entity['API_KEY'];
    const poloniexApiSecret = entity['SECRET'];
    const poloniexClient = new Poloniex(poloniexApiKey, poloniexApiSecret);
    const dateTime = new Date();

    poloniexClient.returnTicker((err, tickerData) => {
      if (!err) {
        let currencyData = [];
        _.forOwn(tickerData, (data, currencyPair) => {
          currencyData.push({
            key: tickerDataStoreKey,
            data: formatCurrencyData(currencyPair, data, dateTime)
          });
        });

        datastore.save(currencyData).then(() => {
          callback();
        }).catch(callback);
      } else {
        callback(err);
      }
    });
  }).catch(callback);
};

const formatCurrencyData = (currencyPair, data, dateTime) => {
  return {
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
}

const getBroadcastTickerChangePromise = () => {

}

const getDeleteOldDataPromise = () => {
  const query = datastore.createQuery('ticker');
  query.filter('location', 'CA');

  return datastore.runQuery(query).then((entities) => {
    // entities[0][datastore.KEY];
  });
}
