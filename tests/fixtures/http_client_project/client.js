const axios = require('axios');
const BASE = process.env.USER_SERVICE_URL;

async function fetchUser(id) {
    return axios.get(BASE + '/v1/users/' + id);
}

async function score(payload) {
    return axios.post('http://user-service/v1/score', payload);
}

module.exports = { fetchUser, score };
