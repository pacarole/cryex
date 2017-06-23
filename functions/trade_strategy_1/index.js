const _ = require('lodash');
const Promise = require('bluebird');
const Poloniex = require('poloniex-api-node');
const datastore = require('@google-cloud/datastore')({ promise: Promise });

const poloniexApiDataStoreKey = datastore.key(['poloniex_api', 'strategy1']);
const accountInfoDataStoreKey = datastore.key(['account_info', 'strategy1']);

const MS_PER_MINUTE = 60000;
const MAX_BUY_DIVIDER = 1/3;

let poloniexClient, currencyData, accountInfo;

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event The Cloud Functions event.
 * @param {!Function} The callback function.
 */
exports.buyOrSell = (event, callback) => {
  const batchRequests = [getPoloniexClient(), getCurrencyData(), datastore.get(accountInfoDataStoreKey)];
  
  Promise.all(batchRequests).then((promiseResults) => {
    poloniexClient = Promise.promisifyAll(promiseResults[0]);
    currencyData = promiseResults[1][0];
    accountInfo = promiseResults[2][0];
    
    chooseToBuyOrSell().then(() => {
      callback();
    }).catch(callback);
  }).catch(callback);
};

const chooseToBuyOrSell = () => {
  if(!accountInfo.lastAction || accountInfo.lastAction == 'SELL') {
    return buy().then(() => {
      return datastore.save({
        key: accountInfoDataStoreKey,
        data: { lastAction: 'BUY' }
      });
    });
  } else {
    return sell().then(() => {
      return datastore.save({
        key: accountInfoDataStoreKey,
        data: { lastAction: 'SELL' }
      });
    });
  }
}

const buy = () => {
  // sort currency data w/ positive slope by slope * volatilityFactor
  let filteredCurrencyData = _.filter(currencyData, (datum) => {
    return datum.slope > 0;
  });
  filteredCurrencyData = _.sortBy(filteredCurrencyData, (datum) => {
    return datum.slope * datum.volatilityFactor;
  });
  filteredCurrencyData.reverse();
  
  let maxBuyCash;

  return Promise.each(filteredCurrencyData, (currencyInfo) => {
    poloniexClient.returnBalancesAsync().then((balances) => {
      let availableCash = parseFloat(balances.USDT);
      if(_.isUndefined(maxBuyCash)) maxBuyCash = availableCash * MAX_BUY_DIVIDER;
      return makeBuyDecision(maxBuyCash, availableCash, currencyInfo);
    });
  });
}

const makeBuyDecision = (maxBuyCash, availableCash, currencyInfo) => {
  const currencyName = currencyInfo[datastore.KEY].name;
  const buyCash = availableCash < maxBuyCash ? availableCash : maxBuyCash;
  const priceIncreasePercentage = (currencyInfo.currentPrice - currencyInfo.pastPrice) / currencyInfo.pastPrice * 100;
  const stabilityThreshold = 5 - 4 * currencyInfo.volatilityFactor;
  const shouldBuy = priceIncreasePercentage > stabilityThreshold;

  if(buyCash > 0 && shouldBuy) {
    const currencyPair = 'USDT_' + currencyName;
    const rate = currencyInfo.currentPrice;
    const amount = buyCash / rate;
    
    return poloniexClient.buyAsync(currencyPair, rate, amount, false /* fillOrKill */, true /* immediateOrCancel */).then(() => {
      return updateAccountInfo(currencyName, rate, rate);
    });
  } else {
    return updateAccountInfo(currencyName, currencyInfo.currentPrice);
  }
}

const sell = () => {
   let filteredCurrencyData = _.filter(currencyData, (datum) => {
    return datum.slope < 0;
  });
  
  return poloniexClient.returnBalancesAsync().then((balances) => {
    return Promise.each(filteredCurrencyData, (currencyInfo) => {
      return makeSellDecision(balances, currencyInfo).then(() => {
        return updateAccountInfo(currencyInfo[datastore.KEY].name, currencyInfo.currentPrice);
      });
    });                
  });
}

const makeSellDecision = (balances, currencyInfo) => {
  const currencyName = currencyInfo[datastore.KEY].name;
  const currencyBalance = parseFloat(balances[currencyName]);
  
  const buyPrice = accountInfo[currencyName + '_buyPrice'];
  const peakPrice = accountInfo[currencyName + '_peakPrice'];
  
  if(buyPrice) {
    const peakPriceDifferential = (peakPrice - currencyInfo.currentPrice) / (peakPrice - buyPrice) * 100;
    const stabilityThreshold = 15 - 5 * currencyInfo.volatilityFactor;
    const shouldSell = peakPriceDifferential > stabilityThreshold;
    
    if(currencyBalance > 0 && shouldSell) {
      const currencyPair = 'USDT_' + currencyName;
      const rate = currencyInfo.currentPrice;
      const amount = currencyBalance / rate;

      return poloniexClient.sellAsync(currencyPair, rate, amount, false /* fillOrKill */, true /* immediateOrCancel */);
    } else {
      return Promise.resolve();
    }
  } else {
    return Promise.resolve();
  }
}

const getPoloniexClient = () => {
  return new Promise((resolve, reject) => {
    datastore.get(poloniexApiDataStoreKey).then((entity) => {
      const poloniexApiKey = entity[0]['API_KEY'];
      const poloniexApiSecret = entity[0]['SECRET'];
      const poloniexClient = new Poloniex(poloniexApiKey, poloniexApiSecret);
      resolve(poloniexClient);
    }).catch(reject);
  });
}

const updateAccountInfo = (currencyName, currentPrice, newBuyPrice) => {
  const buyPriceProp = currencyName + '_buyPrice';
  const peakPriceProp = currencyName + '_peakPrice';
  const lowPriceProp = currencyName + '_lowPrice';
  
  let newAccountInfo = {};
  
  if(newBuyPrice) {
    newAccountInfo[buyPriceProp] = currentPrice;
    newAccountInfo[peakPriceProp] = currentPrice;
    newAccountInfo[lowPriceProp] = currentPrice;
  } else if(accountInfo[buyPriceProp]) {
    if(currentPrice > accountInfo[peakPriceProp]) newAccountInfo[peakPriceProp] = currentPrice;
    if(currentPrice < accountInfo[lowPriceProp]) newAccountInfo[lowPriceProp] = currentPrice;    
  }
  
  if(_.size(newAccountInfo) > 0) {
    datastore.save({
      key: accountInfoDataStoreKey,
      data: newAccountInfo
    });
  } else {
    return Promise.resolve();
  }
}

const getCurrencyData = () => {
  const query = datastore.createQuery('currency_data');
  return datastore.runQuery(query);
}
