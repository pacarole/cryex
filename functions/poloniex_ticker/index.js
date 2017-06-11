const Poloniex = require('poloniex-api-node');
const datastore = require('@google-cloud/datastore')();

const poloniexApiKey = datastore.key(['poliniex_api', 'ticker']);

/**
 * Triggered from a message on a Cloud Pub/Sub topic.
 *
 * @param {!Object} event The Cloud Functions event.
 * @param {!Function} The callback function.
 */
exports.update = function update(event, callback) {

  datastore.get(poloniexApiKey, function(err, entity) {
    if(err) {
      callback(err);
    } else {
      const poloniexApiKey = entity['API_KEY'];
      const poloniexApiSecret = entity['SECRET'];
      const poloniexClient = new Poloniex(poloniexApiKey, poloniexApiSecret);

      poloniexClient.returnTicker(function(err, ticker) {
        if (!err) console.log(ticker);
        callback();
      });
    }
  });
};
