var ByteBuffer = require('bytebuffer')
	, CRC = require('crc')
	, PSON = require('pson')
	, bindexOf = require('buffer-indexof')
	, type_cast = require('./type_cast')	
	, Long = require('long');

const pson_ = new PSON.StaticPair();
const LBF_Ver = 0x01
	, LBF_Mark = 0x51c0
	, LBF_FLAG_LEN = 8
	, LBF_EVENT_HEADER_LEN = 12
	// 8 bits CRC
	, LBF_EVENT_CRC_LEN = 1
	, LBF_EMPTY_EVENT_LEN = LBF_EVENT_HEADER_LEN + LBF_EVENT_CRC_LEN;

var flag_ = function (metric_version){
	var flag = new ByteBuffer(8);

	flag.writeUint16(LBF_Mark);
	flag.writeUint16(LBF_Ver);
	flag.writeUint16(metric_version);

	// reserved to Zero
	flag.fill(0);

	// reset
	flag.reset();

	return flag;
}

var readFlag_ = function (buffer){
	return buffer.readBytes(LBF_FLAG_LEN);
}

var parseFlag_ = function (flag_buffer){
	var buffer = ByteBuffer.wrap(flag_buffer);
	var flag = {
		"mark" : buffer.readUint16(),
		"ver" : buffer.readUint16(),
		"metric_ver" : buffer.readUint16(),
		"reserved" : buffer.readUint16()
	}

	return flag;
}

var header_ = function (timestamp, chunk_len){
	var buffer = new ByteBuffer(LBF_EVENT_HEADER_LEN);

	buffer.writeUint64(timestamp);
	buffer.writeUint32(chunk_len);
	buffer.reset();

	return buffer;
}

var readHeader_ = function (buffer){
	return buffer.readBytes(LBF_EVENT_HEADER_LEN);
}

var parseHeader_ = function (header_buffer){
	var buffer = ByteBuffer.wrap(header_buffer);
	return {
		"timestamp" : Long.fromValue(buffer.readUint64()).toNumber(),
		"chunk_len" : buffer.readUint32()
	}	
}

var crc_ = function (header){
	return CRC.crc8(header.toBuffer());
}

var readCrc_ = function (buffer){
	return buffer.readUint8();
}

var encodeEventBody_ = function (data, metric){
	var chunk = new Array(metric.keys.length);
	metric.keys.forEach(function (field, i){
		var val = data[field.name];
		if (val != undefined){
			chunk[i] = type_cast(val, field.type);
		}
	});

	return pson_.encode(chunk).toBuffer();
}

var decodeEventBody_ = function (chunk){
	return pson_.decode(chunk.toBuffer());
}

var readChunk_ = function (buffer, header){
	return buffer.readBytes(header.chunk_len);
}

var readEvent_ = function (buffer){
	if (buffer.remaining() < LBF_EMPTY_EVENT_LEN)
		return null;

	// old offset for rollback
	var offset = buffer.offset;

	// read header, chunk with buffer
	var header = readHeader_(buffer);
	var parsed_header = parseHeader_(header);
	// check header
	if ((parsed_header.chunk_len + offset + LBF_EMPTY_EVENT_LEN) > buffer.limit){
		// rollback
		buffer.offset = offset;
		throw new Error("ER_EVENT_HEADER_DAMAGE");		
	}

	// read chunk	
	var chunk = readChunk_(buffer, parsed_header);

	// read crc && check
	var crc = readCrc_(buffer);
	if (crc != crc_(header)){
		// rollback
		buffer.offset = offset;
		throw new Error("ER_EVENT_CRC_FAIL");
	}

	if (!parsed_header.chunk_len){
		// skip
		chunk = null;
	}

	return {
		"header" : parsed_header,
		"chunk" : chunk
	}
}

var event_ = function (timestamp, data, metric){
	var chunk = encodeEventBody_(data, metric);
	var header = header_(timestamp, chunk.length);
	var crc = new ByteBuffer(LBF_EVENT_CRC_LEN);
	crc.writeUint8(crc_(header));
	crc.reset();

	return ByteBuffer.concat([header, chunk, crc]);
}

var emptyEvent_ = function (){
	var header = header_(0, 0);
	var crc = new ByteBuffer(LBF_EVENT_CRC_LEN);
	crc.writeUint8(crc_(header));
	crc.reset();

	return ByteBuffer.concat([header, crc]);
}

var findNextEmptyEvent_ = function (buffer){
	var empty = emptyEvent_().toBuffer();
	var idx = bindexOf(buffer.toBuffer(), empty);
	if (idx == -1){ // not found
		buffer.offset = buffer.limit;
		return null;
	}
	else{
		buffer.offset += idx;
		return readEvent_(buffer);
	}
}

/*
	export modules
*/
exports.LBF_Mark = LBF_Mark;
exports.LBF_Ver = LBF_Ver;
exports.LBF_FLAG_LEN = LBF_FLAG_LEN;
exports.LBF_EVENT_HEADER_LEN = LBF_EVENT_HEADER_LEN;
exports.LBF_EVENT_CRC_LEN = LBF_EVENT_CRC_LEN;
exports.LBF_EMPTY_EVENT_LEN = LBF_EMPTY_EVENT_LEN;

exports.flag = flag_;
exports.readFlag = function (buffer){
	return parseFlag_(readFlag_(buffer));
};

exports.isMatchedFlag = function (flag){
	return flag.mark == LBF_Mark;
}

exports.wrap = ByteBuffer.wrap;

exports.Event = {};

exports.Event.readHeader = function (buffer){
	parseHeader_(readHeader_(buffer));
};

exports.Event.readCRC =readCrc_;
exports.Event.parseBody = decodeEventBody_;
exports.Event.nextOne = readEvent_;
exports.Event.nextEmptyOne = findNextEmptyEvent_;
exports.Event.encape = function (timestamp, data, metric){
	return event_(timestamp, data ? data : {}, metric).toBuffer();
}
exports.Event.encapeEmptyEvent = function (){
	return emptyEvent_().toBuffer();
}