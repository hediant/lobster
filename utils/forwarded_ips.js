module.exports = forwarded_ips;

/**
 * 如果使用了ngnix做代理，需要进行如下设置才能获取都真实ip,nginx.conf
        proxy_set_header X-Real-IP $remote_addr;
        proxy_set_header X-Forwarded-For $proxy_add_x_forwarded_for;
        proxy_set_header Host $http_host;
        proxy_set_header X-NginX-Proxy true;
* 另外又使用了一层request 代理,这一层代理不改把ngnix头丢弃，否则无法获得
 * Get all addresses in the request, using the `X-Forwarded-For` header.
 *
 * @param {object} req
 * @return {array}
 * 数组的最后一个元素是第一个转发ip,倒数第二个是第二级代理ip ...
 * 如果没经过转发直接取req.connection.remoteAddress
 * @public
 */

function forwarded_ips(req) {
  if (!req) {
    throw new TypeError('argument req is required')
  }

  // simple header parsing
  var proxyAddrs = (req.headers['x-forwarded-for'] || '')
    .split(/ *, */)
    .filter(Boolean)
    .reverse();
  var socketAddr = req.connection.remoteAddress;
  var addrs = [socketAddr].concat(proxyAddrs);

  // return all addresses
  return addrs;
}