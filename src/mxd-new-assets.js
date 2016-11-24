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

    let lastRunAssets = new Set();
    let timeoutHandle;

    async function RUN() {
      node.log('start check for new assets');

      const queries = [];

      if (['all', 'movies'].includes(config.content)) {
        node.log('include check for movies');
        const query = (new AssetsQuery())
          .filter('movies');
        queries.push(query);
      }

      if (['all', 'seasons'].includes(config.content)) {
        node.log('include check for seasons');
        const query = (new AssetsQuery())
          .filter('seasons');
        queries.push(query);
      }

      if (config.area !== 'all') {
        node.log(`restrict area to ${config.area}`);
        queries.forEach((query) => {
          query
            .filter({ package: 'hasPackageContent', store: 'availableWithoutPackage' }[config.area]);
        });
      }

      const requests = queries.map((query) => {
        query
          .filter('new')
          .filter('notUnlisted')
          .sort('activeLicenseStart', 'desc');
        return heimdall.getAssets(query);
      });
      let responses;
      try {
        responses = await Promise.all(requests);
      } catch (e) {
        node.warn(`requesting error, skip this run (${e.message})`);
        return;
      }

      const assets = responses.reduce((a, b) => a.concat(b));
      if (assets.length === 0) {
        node.warn('no assets in the responses');
        return;
      }

      let firstRun = false;
      if (!lastRunAssets.size) {
        node.log('first run');
        firstRun = true;
      }
      const currentRunAssets = new Set();
      assets.forEach((asset) => {
        if (!firstRun && !lastRunAssets.has(asset.id)) {
          node.log(`send new asset (id: ${asset.id})`);
          node.send({ payload: asset });
        }
        currentRunAssets.add(asset.id);
      });
      lastRunAssets = currentRunAssets;

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
