module.exports = {
        jobrunner: require('./jobrunner'),
        ArrayKeyGenerator: require('./keygen/ArrayKeyGenerator.js'),
        FindKeyGenerator: require('./keygen/FindKeyGenerator.js'),
        StorageHourlyKeyGenerator:
                require('./keygen/StorageHourlyKeyGenerator.js')
};
