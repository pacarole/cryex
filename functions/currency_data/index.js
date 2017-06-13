const _ = require('lodash');
const Promise = require('bluebird');
const stats = require('simple-statistics');
const datastore = require('@google-cloud/datastore')({ promise: Promise });
const pubsub = require('@google-cloud/pubsub')({ promise: Promise });

const currencyChangeBroadcastTopic = pubsub.topic('new-currency-data');

const CURRENCY_AGGREGATION_MINUTES = 15;
const MS_PER_MINUTE = 60000;
const currencies = ['BTC', 'ETH', 'ETC', 'XRP', 'NXT', 'LTC', 'ZEC', 'DASH', 'STR', 'XMR', 'REP'];

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


  /*
    let samples = [[0, 0], [1, 1]];
    let regression = stats.linearRegression(samples);
    let slope = regression.m;
    let regressionLine = stats.linearRegressionLine(regression);
    let volatility = stats.rSquared(samples, regressionLine); // = 1 this line is a perfect fit
  */
}

const broadcastCurrencyDataChange = () => {
  return currencyChangeBroadcastTopic.publish('currency data updated');
}

const getCurrencyEntries = (currency) => {
  const query = datastore.createQuery('ticker');
  const currentDate = new Date();
  const maxAgeDate = new Date(currentDate.getTime() - CURRENCY_AGGREGATION_MINUTES * MS_PER_MINUTE);
  query.filter('dateTime', '<', maxAgeDate);
  query.filter('currencyPair', 'USDT_'+currency);
  query.order('dataTime', { descending: true });

  return datastore.runQuery(query);
}

const deleteTickerEntries = (entities) => {
  const keys = _.map(entities[0], datastore.KEY);
  return datastore.delete(keys);
}
