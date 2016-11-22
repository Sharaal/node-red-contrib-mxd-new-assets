const { AssetsQuery, Heimdall } = require('mxd-heimdall');

module.exports = (RED) => {
  RED.nodes.registerType('mxd-new-assets', function NODE(config) {
    RED.nodes.createNode(this, config);
    const node = this;

    if (!config.apikey || !config.appid || !config.interval) {
      node.error('config is missing');
    }
    node.log(`initialize mxd-new-assets node with an interval of ${config.interval} seconds`);

    const heimdall = new Heimdall({ apikey: config.apikey, appid: config.appid });

    const oldAssets = new Set();
    let timeoutHandle;

    async function RUN() {
      node.log('start check for new assets');

      const requests = [];

      if (config['area-package'] && config['type-movies']) {
        node.log('include check for package movies');
        requests.push(
          heimdall.getAssets((new AssetsQuery())
            .filter('new')
            .filter('notUnlisted')
            .filter('hasPackageContent')
            .filter('movies')
            .sort('activeLicenseStart', 'desc'))
        );
      }

      if (config['area-package'] && config['type-seasons']) {
        node.log('include check for package seasons');
        requests.push(
          heimdall.getAssets((new AssetsQuery())
            .filter('new')
            .filter('notUnlisted')
            .filter('hasPackageContent')
            .filter('seasons')
            .sort('activeLicenseStart', 'desc'))
        );
      }

      if (config['area-store'] && config['type-movies']) {
        node.log('include check for store movies');
        requests.push(
          heimdall.getAssets((new AssetsQuery())
            .filter('new')
            .filter('notUnlisted')
            .filter('availableWithoutPackage')
            .filter('movies')
            .sort('activeLicenseStart', 'desc'))
        );
      }

      if (config['area-store'] && config['type-seasons']) {
        node.log('include check for store seasons');
        requests.push(
          heimdall.getAssets((new AssetsQuery())
            .filter('new')
            .filter('notUnlisted')
            .filter('availableWithoutPackage')
            .filter('seasons')
            .sort('activeLicenseStart', 'desc'))
        );
      }

      if (requests.length === 0) {
        node.warn('need at least one area and type combination enabled');
        return;
      }

      const responses = await Promise.all(requests);

      const assets = responses.reduce((a, b) => a.concat(b));
      if (assets.length === 0) {
        node.warn('no assets in the responses');
        return;
      }
      if (oldAssets.size) {
        assets.forEach((asset) => {
          if (!oldAssets.has(asset.id)) {
            node.log(`send new asset (id: ${asset.id})`);
            node.send({ payload: asset });
            oldAssets.add(asset.id);
          }
        });
      } else {
        node.log('first run');
        assets.forEach((asset) => {
          oldAssets.add(asset.id);
        });
      }
      node.log('end check for new assets');
      timeoutHandle = setTimeout(RUN, config.interval * 1000);
    }

    process.nextTick(RUN);

    node.on('close', () => {
      if (timeoutHandle) {
        clearTimeout(timeoutHandle);
      }
    });
  });
};
