const _ = require('lodash');
const Poloniex = require('poloniex-api-node');
const Promise = require('bluebird');
const datastore = require('@google-cloud/datastore')({ promise: Promise });
const pubsub = require('@google-cloud/pubsub')({ promise: Promise });

const poloniexApiDataStoreKey = datastore.key(['poloniex_api', 'ticker']);
const tickerChangeBroadcastTopic = pubsub.topic('new-ticker-data');

const TICKER_MAX_AGE_MINUTES = 20;
const MS_PER_MINUTE = 60000;

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
            key: datastore.key(['ticker']),
            data: formatCurrencyData(currencyPair, data, dateTime)
          });
        });

        datastore.save(currencyData)
          .then(broadcastTickerChange)
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
  return tickerChangeBroadcastTopic.publish('ticker data updated');
}

const getOldTickerEntries = () => {
  const query = datastore.createQuery('ticker');
  const currentDate = new Date();
  const maxAgeDate = new Date(currentDate.getTime() - TICKER_MAX_AGE_MINUTES * MS_PER_MINUTE);
  query.filter('dateTime', '<', maxAgeDate);

  return datastore.runQuery(query);
}

const deleteTickerEntries = (entities) => {
  console.log("HERE ARE ENTITIES", entities);
  console.log("HERE IS THE KEY", datastore.KEY);
  const keys = _.map(entities, datastore.KEY);
  console.log("AND THE MAP", keys);
  return datastore.delete(keys);
}
