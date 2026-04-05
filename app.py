const LINE_REPLY_URL = 'https://api.line.me/v2/bot/message/reply';
const GOOGLE_TRANSLATE_URL = 'https://translation.googleapis.com/language/translate/v2';

function doGet() {
  return ContentService
    .createTextOutput('LINE webhook is live')
    .setMimeType(ContentService.MimeType.TEXT);
}

function doPost(e) {
  try {
    const props = PropertiesService.getScriptProperties();
    const CHANNEL_ACCESS_TOKEN = props.getProperty('LINE_CHANNEL_ACCESS_TOKEN');
    const CHANNEL_SECRET = props.getProperty('LINE_CHANNEL_SECRET');
    const GOOGLE_API_KEY = props.getProperty('GOOGLE_API_KEY');

    if (!CHANNEL_ACCESS_TOKEN) {
      return jsonOutput({ ok: false, error: 'missing_LINE_CHANNEL_ACCESS_TOKEN' });
    }
    if (!CHANNEL_SECRET) {
      return jsonOutput({ ok: false, error: 'missing_LINE_CHANNEL_SECRET' });
    }
    if (!GOOGLE_API_KEY) {
      return jsonOutput({ ok: false, error: 'missing_GOOGLE_API_KEY' });
    }
    if (!e || !e.postData || !e.postData.contents) {
      return jsonOutput({ ok: false, error: 'missing_postData' });
    }

    const rawBody = e.postData.contents;
    const signature = getHeaderCaseInsensitive_(e, 'X-Line-Signature');

    if (!signature) {
      return jsonOutput({ ok: false, error: 'missing_X-Line-Signature' });
    }

    const isValid = verifyLineSignature_(rawBody, signature, CHANNEL_SECRET);
    if (!isValid) {
      return jsonOutput({ ok: false, error: 'invalid_signature' });
    }

    const body = JSON.parse(rawBody);
    const events = body.events || [];

    for (let i = 0; i < events.length; i++) {
      const event = events[i];
      handleEvent_(event, CHANNEL_ACCESS_TOKEN, GOOGLE_API_KEY);
    }

    return jsonOutput({ ok: true });
  } catch (err) {
    return jsonOutput({
      ok: false,
      error: 'server_exception',
      message: String(err)
    });
  }
}

function handleEvent_(event, channelAccessToken, googleApiKey) {
  const eventType = event.type || '';
  const replyToken = event.replyToken || '';
  const source = event.source || {};
  const message = event.message || {};
  const messageType = message.type || '';
  const userText = (message.text || '').trim();

  if (eventType !== 'message') return;
  if (messageType !== 'text') return;
  if (!replyToken) return;

  if (userText.toLowerCase() === 'hi') {
    replyText_(replyToken, 'BOT OK', channelAccessToken);
    return;
  }

  if (userText.toLowerCase() === 'help') {
    replyText_(
      replyToken,
      'Lệnh:\nhi\nhelp\n/zh nội dung\n/vi nội dung\n/id nội dung\n/en nội dung',
      channelAccessToken
    );
    return;
  }

  const translated = handleTranslateCommand_(userText, googleApiKey);

  if (translated) {
    replyText_(replyToken, translated, channelAccessToken);
    return;
  }

  replyText_(replyToken, 'Bạn gửi: ' + userText, channelAccessToken);
}

function handleTranslateCommand_(userText, googleApiKey) {
  const text = userText.trim();

  if (text.startsWith('/zh ')) {
    const sourceText = text.slice(4).trim();
    if (!sourceText) return 'Cú pháp đúng: /zh nội dung';
    return '[AUTO → zh-TW]\n' + translateText_(sourceText, 'zh-TW', googleApiKey);
  }

  if (text.startsWith('/vi ')) {
    const sourceText = text.slice(4).trim();
    if (!sourceText) return 'Cú pháp đúng: /vi nội dung';
    return '[AUTO → vi]\n' + translateText_(sourceText, 'vi', googleApiKey);
  }

  if (text.startsWith('/id ')) {
    const sourceText = text.slice(4).trim();
    if (!sourceText) return 'Cú pháp đúng: /id nội dung';
    return '[AUTO → id]\n' + translateText_(sourceText, 'id', googleApiKey);
  }

  if (text.startsWith('/en ')) {
    const sourceText = text.slice(4).trim();
    if (!sourceText) return 'Cú pháp đúng: /en nội dung';
    return '[AUTO → en]\n' + translateText_(sourceText, 'en', googleApiKey);
  }

  return '';
}

function translateText_(text, targetLang, googleApiKey) {
  const payload = {
    q: text,
    target: targetLang,
    format: 'text'
  };

  const options = {
    method: 'post',
    payload: payload,
    muteHttpExceptions: true
  };

  const url = GOOGLE_TRANSLATE_URL + '?key=' + encodeURIComponent(googleApiKey);
  const res = UrlFetchApp.fetch(url, options);
  const code = res.getResponseCode();
  const body = res.getContentText();

  if (code !== 200) {
    return '[LỖI DỊCH] HTTP ' + code + '\n' + body;
  }

  const data = JSON.parse(body);
  const translated =
    (((data || {}).data || {}).translations || [])[0]?.translatedText || '';

  if (!translated) {
    return '[LỖI DỊCH] Không có dữ liệu trả về';
  }

  return decodeHtml_(translated);
}

function replyText_(replyToken, text, channelAccessToken) {
  const payload = {
    replyToken: replyToken,
    messages: [
      {
        type: 'text',
        text: String(text).slice(0, 5000)
      }
    ]
  };

  const options = {
    method: 'post',
    contentType: 'application/json',
    headers: {
      Authorization: 'Bearer ' + channelAccessToken
    },
    payload: JSON.stringify(payload),
    muteHttpExceptions: true
  };

  return UrlFetchApp.fetch(LINE_REPLY_URL, options);
}

function verifyLineSignature_(rawBody, xLineSignature, channelSecret) {
  const signatureBytes = Utilities.computeHmacSha256Signature(rawBody, channelSecret);
  const encodedSignature = Utilities.base64Encode(signatureBytes);
  return encodedSignature === xLineSignature;
}

function getHeaderCaseInsensitive_(e, headerName) {
  if (!e || !e.postData) return '';
  if (e.parameter && e.parameter[headerName]) return e.parameter[headerName];

  const headers = e.headers || e.header || {};
  const keys = Object.keys(headers);
  const target = headerName.toLowerCase();

  for (let i = 0; i < keys.length; i++) {
    if (String(keys[i]).toLowerCase() === target) {
      return headers[keys[i]];
    }
  }
  return '';
}

function decodeHtml_(text) {
  return text
    .replace(/&amp;/g, '&')
    .replace(/&lt;/g, '<')
    .replace(/&gt;/g, '>')
    .replace(/&quot;/g, '"')
    .replace(/&#39;/g, "'");
}

function jsonOutput(obj) {
  return ContentService
    .createTextOutput(JSON.stringify(obj))
    .setMimeType(ContentService.MimeType.JSON);
}
