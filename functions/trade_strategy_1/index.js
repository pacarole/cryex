const _ = require('lodash');
const Promise = require('bluebird');
const datastore = require('@google-cloud/datastore')({ promise: Promise });

const poloniexApiDataStoreKey = datastore.key(['poloniex_api', 'strategy1']);
const accountInfoDataStoreKey = datastore.key(['account_info', 'strategy1']);

const MS_PER_MINUTE = 60000;
const MAX_BUY_DIVIDER = 1/3;

let poloniexClientSingleton, poloniexReturnBalances, poloniexBuy, poloniexSell;

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event The Cloud Functions event.
 * @param {!Function} The callback function.
 */
exports.buyOrSell = (event, callback) => {
  const batchRequests = [getPoloniexClient(), getCurrencyData(), datastore.get(accountInfoDataStoreKey)];
  
  Promise.all(batchRequests).then(([poloniexClient, currencyDataEntities, accountInfo]) => {
    poloniexClientSingleton = poloniexClient;
    poloniexReturnBalances = Promise.promisify(poloniexClientSingleton.returnBalances);
    poloniexBuy = Promise.promisify(poloniexClientSingleton.buy);
    poloniexSel = Promise.promisify(poloniexClientSingleton.sell);
   
    chooseToBuyOrSell(currencyDataEntities[0], accountInfo).then(() => {
      callback();
    }).catch(callback);
  }).catch(callback);
};

const chooseToBuyOrSell = (currencyData, accountInfo) => {
  if(!accountInfo.lastAction || accountInfo.lastAction == 'SELL') {
    return buy(currencyData, accountInfo).then(() {
      return datastore.update({
        key: accountInfoDataStoreKey,
        data: { lastAction: 'BUY' };
      });
    });
  } else {
    return sell(currencyData, accountInfo).then(() {
      return datastore.update({
        key: accountInfoDataStoreKey,
        data: { lastAction: 'SELL' };
      });
    });
  }
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
  
  return new Promise((resolve, reject) => {
    poloniexReturnBalances.then((balances) => {
      let maxBuyCash;
      
      Promise.each(filteredCurrencyData, (currencyInfo) => {
        return poloniexReturnBalances.then((balances) => {
          let availableCash = parseFloat(balances.USDT);
          if(_.isUndefined(maxBuyCash)) maxBuyCash = availableCash * MAX_BUY_DIVIDER;
          
          return makeBuyDecision(maxBuyCash, availableCash, currencyInfo, accountInfo);
        });
      }).then(resolve).catch(reject);
    }).catch(reject);                  
  });
}

const makeBuyDecision = (maxBuyCash, availableCash, currencyInfo, accountInfo) => {
  const buyCash = availableCash < maxBuyCash ? availableCash : maxBuyCash;
  const priceIncreasePercentage = (currencyInfo.currentPrice - currencyInfo.pastPrice) / currencyInfo.pastPrice * 100;
  const stabilityThreshold = 5 - 4 * currencyInfo.volatilityFactor;
  const shouldBuy = priceIncreasePercentage > stabilityThreshold;
  
  if(buyCash > 0 && shouldBuy) {
    const currencyPair = 'USDT_' + currencyInfo.name;
    const rate = currencyInfo.currentPrice;
    const amount = buyCash / rate;
    
    return poloniexBuy(currencyPair, rate, amount, false /* fillOrKill */, true /* immediateOrCancel */).then(() => {
      return updateAccountInfo(accountInfo, currencyInfo.name, rate, rate);
    }); 
  } else {
    return updateAccountInfo(accountInfo, currencyInfo.name, currencyInfo.currentPrice);
  }
}

const sell = (currencyData, accountInfo) => {
   let filteredCurrencyData = _.filter(currencyData, (datum) => {
    return datum.slope < 0;
  });
  
  return new Promise((resolve, reject) => {
    poloniexReturnBalances.then((balances) => {
      Promise.each(filteredCurrencyData, (currencyInfo) => {
        return makeSellDecision(balances, currencyInfo, accountInfo).then(() => {
          return updateAccountInfo(accountInfo, currencyInfo.name, currencyInfo.currentPrice);
        });
      }).then(resolve).catch(reject);
    }).catch(reject);                  
  });
}

const makeSellDecision = (balances, currencyInfo, accountInfo) => {
  const currencyName = currencyInfo.name;
  const currencyBalance = balances[currencyName];
  
  const buyPrice = accountInfo[currencyName + '_buyPrice'];
  const peakPrice = accountInfo[currencyName + '_peakPrice'];
  
  const peakPriceDifferential = (peakPrice - currencyInfo.currentPrice) / (peakPrice - buyPrice) * 100;
  const stabilityThreshold = 15 - 5 * currencyInfo.volatilityFactor;
  const shouldSell = peakPriceDifferential > stabilityThreshold;
  
  if(currencyBalance > 0 && shouldSell) {
    const currencyPair = 'USDT_' + currencyName;
    const rate = currencyInfo.currentPrice;
    const amount = currencyBalance / rate;
    
    return poloniexSell(currencyPair, rate, amount, false /* fillOrKill */, true /* immediateOrCancel */); 
  } else {
    Promise.resolve();
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

const updateAccountInfo = (accountInfo, currencyName, currentPrice, newBuyPrice) => {
  const buyPriceProp = currencyName + '_buyPrice';
  const peakPriceProp = currencyName + '_peakPrice';
  const lowPriceProp = currencyName + '_lowPrice';
  
  let newAccountInfo = {};
  
  if(newBuyPrice) {
    newAccountInfo[buyPriceProp] = currentPrice;
    newAccountInfo[peakPriceProp] = currentPrice;
    newAccountInfo[lowPriceProp] = currentPrice;
  } else {
    if(currentPrice > accountInfo[peakPriceProp]) newAccountInfo[peakPriceProp] = currentPrice;
    if(currentPrice < accountInfo[lowPriceProp]) newAccountInfo[lowPriceProp] = currentPrice;    
  }
  
  if(_.size(newAccountInfo) > 0) {
    datastore.update({
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
