const _ = require('lodash');
const Promise = require('bluebird');
const stats = require('simple-statistics');
const datastore = require('@google-cloud/datastore')({ promise: Promise });
const pubsub = require('@google-cloud/pubsub')({ promise: Promise });

const CURRENCY_AGGREGATION_MINUTES = 10;
const SHORT_CURRENCY_AGGREGATION_MINUTES = 5;
const MS_PER_MINUTE = 60000;

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event The Cloud Functions event.
 * @param {!Function} The callback function.
 */
exports.updateUSDT = (event, callback) => {
  updateForCurrency('USDT')
    .then(() => {
      callback();
    }).catch(callback);
};

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event The Cloud Functions event.
 * @param {!Function} The callback function.
 */
exports.updateBTC = (event, callback) => {
  updateForCurrency('BTC')
    .then(() => {
      callback();
    }).catch(callback);
};

const updateForCurrency = (baseCurrency) => {
  return getCurrencyEntries(baseCurrency)
    .then((entities) => {
      aggregateAndSaveCurrencyData(baseCurrency, entities);
    }).then(() => {
      broadcastCurrencyDataChange(baseCurrency);
    });
};


const aggregateAndSaveCurrencyData = (baseCurrency, entities) => {
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
    primaryCurrencyData.shortPercentageGain = shortCurrencyData.percentageGain;
    primaryCurrencyData.shortVolatilityFactor = shortCurrencyData.volatilityFactor;

    currencyData.push({
      key: datastore.key(['currency_data_' + baseCurrency, currency]),
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

  return {
    currentPrice: newestRow.last,
    pastPrice: oldestRow.last,
    percentageGain: (regressionLine(slopeSpan) - regression.b) / regression.b * 100,
    volatilityFactor: stats.rSquared(samples, regressionLine) || 0,
    volume24h: newestRow.baseVolume,
    highestBid: newestRow.highestBid
  };
}

const broadcastCurrencyDataChange = (baseCurrency) => {
  const currencyChangeBroadcastTopic = pubsub.topic('new-currency-data-' + baseCurrency);
  return currencyChangeBroadcastTopic.publish('currency data updated');
}

const getCurrencyEntries = (baseCurrency) => {
  const query = datastore.createQuery('ticker');
  query.filter('currencyPair', '>', baseCurrency + '_AAAA');
  query.filter('currencyPair', '<', baseCurrency + '_ZZZZ');

  return datastore.runQuery(query);
}
