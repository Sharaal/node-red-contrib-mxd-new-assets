const DNode = require('node-red-contrib-dnode');

module.exports = DNode.createNode('mxd-new-assets', (dnode) => {
  const { interval, area, content } = dnode.getConfigs(['interval', 'area', 'content']);
  const { AssetsQuery, heimdall } = dnode.getServices('heimdall');

  let lastRunAssets = new Set();
  dnode.onTick(interval, async () => {
    const queries = [];

    if (['all', 'movies'].includes(content)) {
      const query = (new AssetsQuery())
        .filter('movies');
      queries.push(query);
    }
    if (['all', 'seasons'].includes(content)) {
      const query = (new AssetsQuery())
        .filter('seasons');
      queries.push(query);
    }

    if (area !== 'all') {
      queries.forEach((query) => {
        query
          .filter({ package: 'hasPackageContent', store: 'availableWithoutPackage' }[area]);
      });
    }

    const requests = queries.map((query) => {
      query
        .filter('new')
        .filter('notUnlisted')
        .sort('activeLicenseStart', 'desc');
      return heimdall.getAssets(query);
    });
    const responses = await Promise.all(requests);
    const assets = responses.reduce((a, b) => a.concat(b));
    if (assets.length === 0) {
      throw new Error('no assets in the responses');
    }

    let firstRun = false;
    if (!lastRunAssets.size) {
      firstRun = true;
    }
    const currentRunAssets = new Set();
    const newAssets = new Set();
    assets.forEach((asset) => {
      if (!firstRun && !lastRunAssets.has(asset.id)) {
        newAssets.add(asset);
      }
      currentRunAssets.add(asset.id);
    });
    lastRunAssets = currentRunAssets;

    dnode.setStatus('green', `sent ${newAssets.size} new assets`);
    if (newAssets.size > 0) {
      dnode.sendMessage({ payload: Array.from(newAssets) });
    }
  });
});
