const _ = require('lodash');
const Promise = require('bluebird');
const stats = require('simple-statistics');
const datastore = require('@google-cloud/datastore')({ promise: Promise });
const pubsub = require('@google-cloud/pubsub')({ promise: Promise });

const currencyChangeBroadcastTopic = pubsub.topic('new-currency-data');

const CURRENCY_AGGREGATION_MINUTES = 15;
const SHORT_CURRENCY_AGGREGATION_MINUTES = 5;
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
  const currentDate = new Date();
  const maxAgeDate = new Date(currentDate.getTime() - CURRENCY_AGGREGATION_MINUTES * MS_PER_MINUTE);
  const shortMaxAgeDate = new Date(currentDate.getTime() - SHORT_CURRENCY_AGGREGATION_MINUTES * MS_PER_MINUTE);

  const filteredEntities = _.filter(entities[0], (entity) => {
    return entity.dateTime > maxAgeDate;
  });

  const currencyPairs = _.map(filteredEntities, 'currencyPair');
  const uniqueCurrencyPairs = _.uniq(currencyPairs);
  let dataForCurrencies = {};

  _.forEach(uniqueCurrencyPairs, (currencyPair) => {
    let dataForCurrency = _.filter(filteredEntities, (entity) => {
      return entity.currencyPair === currencyPair;
    });

    let currency = currencyPair.split('_')[1];
    dataForCurrencies[currency] = dataForCurrency;
  });

  let currencyData = [];
  _.forOwn(dataForCurrencies, (data, currency) => {
    let primaryCurrencyData = aggregateCurrencyData(currency, data, CURRENCY_AGGREGATION_MINUTES);
    const shortData = _.filter(data, (row) => {
      return row.dateTime > shortMaxAgeDate;
    });
    const shortCurrencyData = aggregateCurrencyData(currency, shortData, SHORT_CURRENCY_AGGREGATION_MINUTES);
    primaryCurrencyData.shortSlopeAngle = shortCurrencyData.slopeAngle;
    
    currencyData.push({
      key: datastore.key(['currency_data', currency]),
      data: primaryCurrencyData
    });
  });

  return datastore.save(currencyData);
}

const aggregateCurrencyData = (currency, data, slopeSpan) => {
  // guarantee order by date
  const tickerRows = _.sortBy(data, (row) => {
    return row.dateTime.getTime();
  });

  const numRows = tickerRows.length;
  const oldestRow = tickerRows[0];
  const newestRow = tickerRows[numRows - 1];
  const fistTimeStamp = oldestRow.dateTime.getTime();
  
  let samples = [];
  _.forEach(tickerRows, (row) => {
    let adjustedTime = row.dateTime.getTime() - fistTimeStamp;
    let adjustedTimeInMinutes = adjustedTime / MS_PER_MINUTE;
    samples.push([ adjustedTimeInMinutes, row.last ]);
  });

  const regression = stats.linearRegression(samples);
  const regressionLine = stats.linearRegressionLine(regression);
  const lastPoint = regressionLine(slopeSpan) - regression.b;
  
  return {
    currentPrice: newestRow.last,
    pastPrice: oldestRow.last,
    slope: regression.m,
    slopeAngle: Math.atan(lastPoint / slopeSpan) * 180 / Math.PI, 
    volatilityFactor: stats.rSquared(samples, regressionLine),
    volume24h: newestRow.baseVolume,
    highestBid: newestRow.highestBid
  };
}

const broadcastCurrencyDataChange = () => {
  return currencyChangeBroadcastTopic.publish('currency data updated');
}

const getCurrencyEntries = (currency) => {
  const query = datastore.createQuery('ticker');
  query.filter('currencyPair', '>', 'USDT_AAAA');
  query.filter('currencyPair', '<', 'USDT_ZZZZ');

  return datastore.runQuery(query);
}
