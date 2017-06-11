const _ = require('lodash');
const Poloniex = require('poloniex-api-node');
const Promise = require('bluebird');
const datastore = require('@google-cloud/datastore')({ promise: Promise });

const poloniexApiDataStoreKey = datastore.key(['poloniex_api', 'ticker']);
const tickerDataStoreKey = datastore.key(['ticker']);

const TICKER_MAX_AGE_MINUTES = 20;

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

        datastore.save(currencyData)
          .then(getOldTickerEntries)
          .then(deleteTickerEntries)
          .then(() => {
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

const broadcastTickerChange = () => {

}

const getOldTickerEntries = () => {
  const query = datastore.createQuery('ticker');
  const currentDate = new Date();
  const maxAgeDate = currentDate.addMinutes(-TICKER_MAX_AGE_MINUTES);
  query.filter('dateTime', '<', maxAgeDate);

  return datastore.runQuery(query);
}

const deleteTickerEntries = (entities) => {
  const keys = _.pluck(entities, datastore.KEY);
  return datastore.delete(keys);
}
