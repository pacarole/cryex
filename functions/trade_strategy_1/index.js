const _ = require('lodash');
const Promise = require('bluebird');
const Poloniex = require('poloniex-api-node');
const datastore = require('@google-cloud/datastore')({ promise: Promise });

const poloniexApiDataStoreKey = datastore.key(['poloniex_api', 'strategy1']);
const accountInfoDataStoreKey = datastore.key(['account_info', 'strategy1']);

const MS_PER_MINUTE = 60000;
const MAX_BUY_DIVIDER = 1/3;

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event The Cloud Functions event.
 * @param {!Function} The callback function.
 */
exports.buyOrSell = (event, callback) => {
  const batchRequests = [getPoloniexClient(), getCurrencyData(), datastore.get(accountInfoDataStoreKey)];
  
  Promise.all(batchRequests).then(([poloniexClient, currencyDataEntities, accountInfo]) => {
    chooseToBuyOrSell(poloniexClient, currencyDataEntities[0], accountInfo).then(() => {
      callback();
    }).catch(callback);
  }).catch(callback);
};

const chooseToBuyOrSell = (poloniexClient, currencyData, accountInfo) => {
  if(!accountInfo.lastAction || accountInfo.lastAction == 'SELL') {
    return buy(poloniexClient, currencyData, accountInfo).then(() => {
      return datastore.update({
        key: accountInfoDataStoreKey,
        data: { lastAction: 'BUY' }
      });
    });
  } else {
    return sell(poloniexClient, currencyData, accountInfo).then(() => {
      return datastore.update({
        key: accountInfoDataStoreKey,
        data: { lastAction: 'SELL' }
      });
    });
  }
}

const buy = (poloniexClient, currencyData, accountInfo) => {
  // sort currency data w/ positive slope by slope * volatilityFactor
  let filteredCurrencyData = _.filter(currencyData, (datum) => {
    return datum.slope > 0;
  });
  filteredCurrencyData = _.sortBy(filteredCurrencyData, (datum) => {
    return datum.slope * datum.volatilityFactor;
  });
  filteredCurrencyData.reverse();
  
  return new Promise((resolve, reject) => {
    poloniexClient.returnBalances((err, balances) => {
      if(err) {
        reject(err);
      } else {
        let maxBuyCash;
      
        Promise.each(filteredCurrencyData, (currencyInfo) => {
          return new Promise((resolve, reject) => {
            poloniexClient.returnBalances((err, balances) => {
              if(err) {
                reject(err);
              } else {
                let availableCash = parseFloat(balances.USDT);
                if(_.isUndefined(maxBuyCash)) maxBuyCash = availableCash * MAX_BUY_DIVIDER;

                makeBuyDecision(poloniexClient, maxBuyCash, availableCash, currencyInfo, accountInfo).then(resolve).catch(reject);
              }
            });
          }); 
        });
      }
    });                
  });
}

const makeBuyDecision = (poloniexClient, maxBuyCash, availableCash, currencyInfo, accountInfo) => {
  const buyCash = availableCash < maxBuyCash ? availableCash : maxBuyCash;
  const priceIncreasePercentage = (currencyInfo.currentPrice - currencyInfo.pastPrice) / currencyInfo.pastPrice * 100;
  const stabilityThreshold = 5 - 4 * currencyInfo.volatilityFactor;
  const shouldBuy = priceIncreasePercentage > stabilityThreshold;
  
  if(buyCash > 0 && shouldBuy) {
    const currencyPair = 'USDT_' + currencyInfo.name;
    const rate = currencyInfo.currentPrice;
    const amount = buyCash / rate;
    
    return new Promise((resolve, reject) => {
      poloniexClient.buy(currencyPair, rate, amount, false /* fillOrKill */, true /* immediateOrCancel */, (err) => {
        if(err) {
          reject(err);
        } else {
          updateAccountInfo(accountInfo, currencyInfo.name, rate, rate).then(resolve).catch(reject);
        }
      });
    });
  } else {
    return updateAccountInfo(accountInfo, currencyInfo.name, currencyInfo.currentPrice);
  }
}

const sell = (poloniexClient, currencyData, accountInfo) => {
   let filteredCurrencyData = _.filter(currencyData, (datum) => {
    return datum.slope < 0;
  });
  
  return new Promise((resolve, reject) => {
    poloniexClient.returnBalances((err, balances) => {
      if(err) {
        reject(err);
      } else {
        Promise.each(filteredCurrencyData, (currencyInfo) => {
          return makeSellDecision(poloniexClient, balances, currencyInfo, accountInfo).then(() => {
            return updateAccountInfo(accountInfo, currencyInfo.name, currencyInfo.currentPrice);
          });
        }).then(resolve).catch(reject);
      }
    });                  
  });
}

const makeSellDecision = (poloniexClient, balances, currencyInfo, accountInfo) => {
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
    
    return new Promise((resolve, reject) => {
      poloniexClient.sell(currencyPair, rate, amount, false /* fillOrKill */, true /* immediateOrCancel */, (err) => {
        if(err) {
          reject(err);
        } else {
          resolve();
        }
      });
    });
  } else {
    Promise.resolve();
  }
}

const getPoloniexClient = () => {
  return new Promise((resolve, reject) => {
    datastore.get(poloniexApiDataStoreKey).then((entity) => {
      const poloniexApiKey = entity['API_KEY'];
      const poloniexApiSecret = entity['SECRET'];
      console.log("SOME LOGGING", poloniexApiKey, poloniexApiSecret, entity);
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
