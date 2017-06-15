const _ = require('lodash');
const Promise = require('bluebird');
const stats = require('simple-statistics');
const datastore = require('@google-cloud/datastore')({ promise: Promise });
const pubsub = require('@google-cloud/pubsub')({ promise: Promise });

const currencyChangeBroadcastTopic = pubsub.topic('new-currency-data');

const CURRENCY_AGGREGATION_MINUTES = 15;
const MS_PER_MINUTE = 60000;

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event The Cloud Functions event.
 * @param {!Function} The callback function.
 */
exports.update = (event, callback) => {
  getCurrencyEntries()
    .then(aggregateAndSaveCurrencyData)
    .then(broadcastCurrencyDataChange)
    .then(() => {
      callback();
    }).catch(callback);
};

const aggregateAndSaveCurrencyData = (entities) => {
  const currencyPairs = _.map(entities[0], 'currencyPairs');
  const uniqueCurrencyPairs = _.uniq(currencyPairs);
  let dataForCurrencies = {};

  _.forEach(uniqueCurrencyPairs, (currencyPair) => {
    let dataForCurrency = _.filter(entities[0], (entity) => {
      return entity.currencyPair === currencyPair;
    });

    let currency = currencyPair.split('_')[1];
    dataForCurrencies[currency] = dataForCurrency;
  });

  let currencyData = [];
  _.forOwn(dataForCurrencies, (data, currency) => {
    currencyData.push({
      key: datastore.key(['currency_data', currency]),
      data: aggregateCurrencyData(currency, data)
    });
  });

  return datastore.save(currencyData);
}

const aggregateCurrencyData = (currency, data) => {
  // guarantee order by date
  const tickerRows = _.sortBy(data, (row) => {
    return row.dateTime.getTime();
  });

  const numRows = tickerRows.length;
  const oldestRow = tickerRows[0];
  const newestRow = tickerRows(numRows - 1);

  let samples = [];
  for(var i=0; i < numRows; i++) {
    samples.push([ i, tickerRows[i].last ]);
  }

  const regression = stats.linearRegression(samples);
  const regressionLine = stats.linearRegressionLine(regression);

  return {
    currentPrice: newestRow.last,
    pastPrice: oldestRow.last,
    slope: regression.m,
    volatilityFactor: stats.rSquared(samples, regressionLine),
    24hVolume: newestRow.baseVolume
  };
}

const broadcastCurrencyDataChange = () => {
  return currencyChangeBroadcastTopic.publish('currency data updated');
}

const getCurrencyEntries = (currency) => {
  const query = datastore.createQuery('ticker');
  const currentDate = new Date();
  const maxAgeDate = new Date(currentDate.getTime() - CURRENCY_AGGREGATION_MINUTES * MS_PER_MINUTE);
  query.filter('dateTime', '>', maxAgeDate);
  query.filter('currencyPair', '>', 'USDT_AAAA');
  query.filter('currencyPair', '<', 'USDT_ZZZZ');
  query.order('dateTime', { descending: true });

  return datastore.runQuery(query);
}
