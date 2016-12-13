module.exports = (RED) => {
  RED.nodes.registerType('mxd-new-assets', function NODE(config) {
    RED.nodes.createNode(this, config);
    const node = this;
    node.status({});

    if (!config.interval) {
      node.status({ fill: 'red', shape: 'dot', text: 'config is missing' });
      node.error('config is missing');
    }
    node.log(`initialize mxd-new-assets node with an interval of ${config.interval} seconds`);

    const { AssetsQuery, heimdall } = RED.nodes.getNode(config.heimdall);

    let lastRunAssets = new Set();
    let timeoutHandle;

    async function RUN() {
      node.status({ fill: 'grey', shape: 'dot', text: `check for new assets...` });
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
        node.status({ fill: 'yellow', shape: 'dot', text: 'requesting error' });
        node.warn(`requesting error, skip this run (${e.message})`);
        return;
      }

      const assets = responses.reduce((a, b) => a.concat(b));
      if (assets.length === 0) {
        node.status({ fill: 'yellow', shape: 'dot', text: 'no assets in the responses' });
        node.warn('no assets in the responses');
        return;
      }

      let firstRun = false;
      if (!lastRunAssets.size) {
        node.log('first run');
        firstRun = true;
      }
      const currentRunAssets = new Set();
      const newAssets = new Set();
      assets.forEach((asset) => {
        if (!firstRun && !lastRunAssets.has(asset.id)) {
          node.log(`new asset (id: ${asset.id})`);
          newAssets.add(asset);
        }
        currentRunAssets.add(asset.id);
      });
      lastRunAssets = currentRunAssets;

      node.status({ fill: 'green', shape: 'dot', text: `sent ${newAssets.size} new assets` });
      if (newAssets.size > 0) {
        node.log(`send ${newAssets.size} new assets`);
        node.send({ payload: Array.from(newAssets) });
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
