const clickhouseUrl = "https://play.clickhouse.com/?user=paste";

const encoder = new TextEncoder();
const decoder = new TextDecoder();

async function fetchJson(url, options) {
    const response = await fetch(url, options);
    if (!response.ok) throw new Error(`HTTP status ${response.status}`);
    return await response.json();
}

function sipHash128(message) {
    function rotl(v, offset, bits) {
        v[offset] = (v[offset] << bits) | (v[offset] >> (64n - bits));
    }

    function compress(v) {
        v[0] += v[1];
        v[2] += v[3];
        rotl(v, 1, 13n);
        rotl(v, 3, 16n);
        v[1] ^= v[0];
        v[3] ^= v[2];
        rotl(v, 0, 32n);
        v[2] += v[1];
        v[0] += v[3];
        rotl(v, 1, 17n);
        rotl(v, 3, 21n);
        v[1] ^= v[2];
        v[3] ^= v[0];
        rotl(v, 2, 32n);
    }

    const view = new DataView(message.buffer);
    let buf = new Uint8Array(new ArrayBuffer(8));
    let v = new BigUint64Array([0x736f6d6570736575n, 0x646f72616e646f6dn, 0x6c7967656e657261n, 0x7465646279746573n]);

    let offset = 0;
    for (; offset < message.length - 7; offset += 8) {
        let word = view.getBigUint64(offset, true);
        v[3] ^= word;
        compress(v);
        compress(v);
        v[0] ^= word;
    }

    buf.set(message.slice(offset));
    buf.fill(0, message.length - offset, 7);
    buf[7] = message.length;

    let word = new DataView(buf.buffer).getBigUint64(0, true);

    v[3] ^= word;
    compress(v);
    compress(v);
    v[0] ^= word;
    v[2] ^= 0xFFn;
    compress(v);
    compress(v);
    compress(v);
    compress(v);

    return ('00000000000000000000000000000000' + ((v[0] ^ v[1]) + ((v[2] ^ v[3]) << 64n)).toString(16)).substr(-32).
        match(/../g).reverse().join('');
}

function getFingerprint(text) {
    const matches = text.match(/\p{L}{4,100}/gu);
    if (!matches) return 'ffffffff';
    return matches
        .map((elem, idx, arr) => idx + 2 < arr.length ? [elem, arr[idx + 1], arr[idx + 2]] : [])
        .filter(elem => elem.length === 3)
        .map(elem => elem.join())
        .filter((elem, idx, arr) => arr.indexOf(elem) === idx)
        .map(elem => sipHash128(encoder.encode(elem)).substr(0, 8))
        .reduce((min, curr) => curr < min ? curr : min, 'ffffffff');
}

async function encrypt(content, key) {
    const aesKey = await window.crypto.subtle.importKey('raw', key, { name: 'AES-CTR', length: 128 }, false, ['encrypt']);
    let plaintext = encoder.encode(content);
    let counter = new Uint8Array(16); // This is okay as long as the key is not reused.
    let encrypted = new Uint8Array(await window.crypto.subtle.encrypt({ name: 'AES-CTR', counter: counter, length: 128 }, aesKey, plaintext));
    return btoa(String.fromCharCode(...encrypted));
}

async function decrypt(content, key) {
    const aesKey = await window.crypto.subtle.importKey('raw', key, { name: 'AES-CTR', length: 128 }, false, ['decrypt']);
    let ciphertext = Uint8Array.from(atob(content), c => c.charCodeAt(0));
    let counter = new Uint8Array(16); // This is okay as long as the key is not reused.
    let decrypted = new Uint8Array(await window.crypto.subtle.decrypt({ name: 'AES-CTR', counter: counter, length: 128 }, aesKey, ciphertext));
    return decoder.decode(decrypted);
}

/*
async function load(fingerprint, hash) {
    const response = await fetchJson(
        clickhouseUrl,
        { method: 'POST', body: `SELECT content, is_encrypted, lower(hex(reinterpretAsFixedString(prev_hash))) AS prev_hash, lower(hex(reinterpretAsFixedString(prev_fingerprint))) AS prev_fingerprint FROM data WHERE fingerprint = reinterpretAsUInt32(unhex('${fingerprint}')) AND hash = reinterpretAsUInt128(unhex('${hash}')) ORDER BY time LIMIT 1 FORMAT JSON` }
    );
    console.log('DEBUG RESPONSE', response);
    const result = response.data[0];
    let content = result.content;
    const isEncrypted = result.is_encrypted;
    if (isEncrypted) {
        const key = Uint8Array.from(atob(window.location.hash.substring(1)), c => c.charCodeAt(0));
        content = await decrypt(content, key);
    }
    return { content, prevHash: result.prev_hash, prevFingerprint: result.prev_fingerprint };
}
*/

async function load(fingerprint, hash, type) {
    const clickhouseUrl = "https://play.clickhouse.com/?user=paste";  // Adjust this URL as needed

    // Construct the query
    const query = `
        SELECT content, is_encrypted
        FROM data
        WHERE fingerprint = reinterpretAsUInt32(unhex('${fingerprint}'))
          AND hash = reinterpretAsUInt128(unhex('${hash}'))
        ORDER BY time
        LIMIT 1
        FORMAT JSON
    `;

    try {
        // Fetch the data from Clickhouse
        const response = await fetch(clickhouseUrl, {
            method: 'POST',
            body: query,
            headers: {
                'Content-Type': 'application/x-www-form-urlencoded',
            },
        });

        if (!response.ok) {
            throw new Error(`HTTP status ${response.status}`);
        }

        const responseText = await response.text();
        // console.log("Response Text:", responseText);

        const json = JSON.parse(responseText);

        if (json.rows < 1) {
            console.error("Paste not found or multiple rows returned.");
            return;
        }

        let content = json.data[0].content || false;
        const is_encrypted = json.data[0].is_encrypted;

        if (is_encrypted) {
            const key = window.location.hash.substring(1); // Extract the key from the URL
            if (!key) {
                console.error("Paste is encrypted, but the URL contains no key.");
                return;
            }
            const keyBytes = base64ToBytes(key);
            content = await decrypt(content, keyBytes);
        }


        return { content, prevHash: hash, prevFingerprint: fingerprint };


    } catch (e) {
        console.error("Error loading data:", e);
    }
}

// Helper functions (adjust as needed)
function base64ToBytes(base64) {
    const binaryString = window.atob(base64);
    const len = binaryString.length;
    const bytes = new Uint8Array(len);
    for (let i = 0; i < len; i++) {
        bytes[i] = binaryString.charCodeAt(i);
    }
    return bytes;
}

async function decrypt(contentBase64, keyBytes) {
    const content = window.atob(contentBase64);
    const decoder = new TextDecoder();
    const encryptedBytes = new Uint8Array(content.length);
    for (let i = 0; i < content.length; i++) {
        encryptedBytes[i] = content.charCodeAt(i);
    }

    const algorithm = { name: 'AES-CTR', counter: new Uint8Array(16), length: 128 };
    const key = await crypto.subtle.importKey('raw', keyBytes, algorithm, false, ['decrypt']);
    const decryptedBytes = await crypto.subtle.decrypt(algorithm, key, encryptedBytes.buffer);
    return decoder.decode(decryptedBytes);
}



async function save(content, prevFingerprint, prevHash, isEncrypted) {
    let text = content;
    let anchor = '';

    if (isEncrypted) {
        const keyBytes = crypto.getRandomValues(new Uint8Array(16));
        text = await encrypt(text, keyBytes);
        anchor = '#' + btoa(String.fromCharCode(...keyBytes));
    }

    const currHash = sipHash128(encoder.encode(text));
    const currFingerprint = getFingerprint(text);

    const response = await fetchJson(
        clickhouseUrl,
        {
            method: 'POST',
            body: `INSERT INTO data (fingerprint_hex, hash_hex, prev_fingerprint_hex, prev_hash_hex, content, is_encrypted) FORMAT JSONEachRow ${JSON.stringify({
                fingerprint_hex: currFingerprint,
                hash_hex: currHash,
                prev_fingerprint_hex: prevFingerprint,
                prev_hash_hex: prevHash,
                content: text,
                is_encrypted: isEncrypted
            })}`
        }
    );

    return `${currFingerprint}/${currHash}${anchor}`;
}

export { load, save, encrypt, decrypt, getFingerprint, sipHash128 };