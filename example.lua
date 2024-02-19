local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))
local item_type = nil
local item_name = nil
local item_value = nil
local item_user = nil

local url_count = 0
local tries = 0
local downloaded = {}
local addedtolist = {}
local abortgrab = false
local killgrab = false
local logged_response = false

local discovered_outlinks = {}
local discovered_items = {}
local bad_items = {}
local ids = {}

local thread_counts = {}

local retry_url = false
local is_initial_url = true

abort_item = function(item)
  abortgrab = true
  --killgrab = true
  if not item then
    item = item_name
  end
  if not bad_items[item] then
    io.stdout:write("Aborting item " .. item .. ".\n")
    io.stdout:flush()
    bad_items[item] = true
  end
end

kill_grab = function(item)
  io.stdout:write("Aborting crawling.\n")
  killgrab = true
end

read_file = function(file)
  if file then
    local f = assert(io.open(file))
    local data = f:read("*all")
    f:close()
    return data
  else
    return ""
  end
end

processed = function(url)
  if downloaded[url] or addedtolist[url] then
    return true
  end
  return false
end

discover_item = function(target, item)
  if not target[item] then
    local a, b = string.match(item, "^([^:]+):(.+)$")
    if a and b and a == "post" then
      discover_item(target, "post-api:" .. b)
    end
--print('discovered', item)
    target[item] = true
    return true
  end
  return false
end

find_item = function(url)
  local value = string.match(url, "/aj/player/item/options%?vid=([a-z0-9]+)$")
  local type_ = "play"
  if not value then
    maybe_type, value = string.match(url, "^https?://[^/]*vbox7%.com/([a-z]+):([a-zA-Z0-9_%%]+)$")
    if maybe_type and value then
      if maybe_type == "article"
        or maybe_type == "quiz"
        or maybe_type == "user"
        or maybe_type == "tag" then
        type_ = maybe_type
      else
        value = nil
      end
    end
  end
  if not value then
    value = string.match(url, "^https://[^/]*vbox7%.com/castsub/([0-9]+)%.vtt$")
    type_ = "subtitle"
  end
  if not value
    and not string.match(url, "^https://i[0-9]+%.vbox7%.com/player/ext%.swf") then
    value = string.match(url, "^https?://(i[0-9]+%.vbox7%.com/.+)")
    type_ = "asset"
  end
  if value and type_ then
    return {
      ["value"]=value,
      ["type"]=type_
    }
  end
end

set_item = function(url)
  found = find_item(url)
  if found then
    item_type = found["type"]
    item_value = found["value"]
    item_name_new = item_type .. ":" .. item_value
    if item_name_new ~= item_name then
      ids = {}
      ids[item_value] = true
      abortgrab = false
      tries = 0
      retry_url = false
      is_initial_url = true
      item_name = item_name_new
      print("Archiving item " .. item_name)
    end
  end
end

allowed = function(url, parenturl)
  if ids[url]
    or string.match(url, "^https://i[0-9]+%.vbox7%.com/player/ext%.swf") then
    return true
  end

  if string.match(url, "^https?://[^/]+/login")
    or string.match(url, "^https?://[^/]+/pm/")
    or string.match(url, "^https?://[^/]+/ ")
    or string.match(url, "^https?://[^/]+/%%20")
    or string.match(url, "[;%?&]order=[a-z]")
    or string.match(url, "^https?://[^/]*%%2[fF]")
    or string.match(url, "^https?://[^/]*doubleclick%.net/") then
    return false
  end

  local found = false
  for pattern, type_ in pairs({
    ["[^a-zA-Z0-9]play:([a-z0-9]+)"]="play",
    ["[^a-zA-Z0-9]user:([a-z0-9_]+)"]="user",
    ["[^a-zA-Z0-9]article:([a-z0-9_]+)"]="article",
    ["[^a-zA-Z0-9]quiz:([a-z0-9_]+)"]="quiz",
    ["[^a-zA-Z0-9]tag:([^;%?&]+)"]="tag",
    ["/castsub/([0-9]+)%.vtt$"]="subtitle",
    ["^https?://(i[0-9]+%.vbox7%.com/.+)"]="asset",
    ["^https?://(i[0-9]+%.vbox7%.com/[^%?]+)"]="asset"
  }) do
    match = string.match(url, pattern)
    if match then
      local new_item = type_ .. ":" .. match
      if new_item ~= item_name then
        discover_item(discovered_items, new_item)
        found = true
      end
    end
  end
  if found and item_type ~= "asset" then
    return false
  end

  if item_type ~= "asset"
    and item_type ~= "tag"
    and string.match(url, item_value) then
    return true
  end

  if item_type == "tag"
    and string.match(url, "tag:([^;%?&]+)") == item_value then
    return true
  end

  if not string.match(url, "^https?://[^/]*vbox7%.com/") then
    discover_item(discovered_outlinks, url)
  end

  return false
end

wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
  local url = urlpos["url"]["url"]
  local html = urlpos["link_expect_html"]

  if allowed(url, parent["url"])
    and not processed(url)
    and string.match(url, "^https://")
    and not addedtolist[url] then
    addedtolist[url] = true
    return true
  end

  return false
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
  local urls = {}
  local html = nil
  local json = nil
  
  downloaded[url] = true

  if abortgrab then
    return {}
  end

  local function percent_encode_url(newurl)
    return string.gsub(
      newurl, "(.)",
      function (s)
        local b = string.byte(s)
        if b < 32 or b > 126 then
          return string.format("%%%02X", b)
        end
        return s
      end
    )
  end

  local function decode_codepoint(newurl)
    newurl = string.gsub(
      newurl, "\\[uU]([0-9a-fA-F][0-9a-fA-F][0-9a-fA-F][0-9a-fA-F])",
      function (s)
        return utf8.char(tonumber(s, 16))
      end
    )
    return newurl
  end

  local function fix_case(newurl)
    if not newurl then
      newurl = ""
    end
    if not string.match(newurl, "^https?://[^/]") then
      return newurl
    end
    if string.match(newurl, "^https?://[^/]+$") then
      newurl = newurl .. "/"
    end
    local a, b = string.match(newurl, "^(https?://[^/]+/)(.*)$")
    return string.lower(a) .. b
  end

  local function check(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    newurl = fix_case(newurl)
    local origurl = url
    if string.len(url) == 0 or string.len(newurl) == 0 then
      return nil
    end
    local url = string.match(newurl, "^([^#]+)")
    local url_ = string.match(url, "^(.-)[%.\\]*$")
    while string.find(url_, "&amp;") do
      url_ = string.gsub(url_, "&amp;", "&")
    end
    if not processed(url_)
      and not processed(url_ .. "/")
      and allowed(url_, origurl) then
      table.insert(urls, {
        url=url_
      })
      addedtolist[url_] = true
      addedtolist[url] = true
    end
  end

  local function checknewurl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "['\"><]") then
      return nil
    end
    if string.match(newurl, "^https?:////") then
      check(string.gsub(newurl, ":////", "://"))
    elseif string.match(newurl, "^https?://") then
      check(newurl)
    elseif string.match(newurl, "^https?:\\/\\?/") then
      check(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^\\/\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^//") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^\\/") then
      checknewurl(string.gsub(newurl, "\\", ""))
    elseif string.match(newurl, "^/") then
      check(urlparse.absolute(url, newurl))
    elseif string.match(newurl, "^%.%./") then
      if string.match(url, "^https?://[^/]+/[^/]+/") then
        check(urlparse.absolute(url, newurl))
      else
        checknewurl(string.match(newurl, "^%.%.(/.+)$"))
      end
    elseif string.match(newurl, "^%./") then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function checknewshorturl(newurl)
    if not newurl then
      newurl = ""
    end
    newurl = decode_codepoint(newurl)
    if string.match(newurl, "^%?") then
      check(urlparse.absolute(url, newurl))
    elseif not (
      string.match(newurl, "^https?:\\?/\\?//?/?")
      or string.match(newurl, "^[/\\]")
      or string.match(newurl, "^%./")
      or string.match(newurl, "^[jJ]ava[sS]cript:")
      or string.match(newurl, "^[mM]ail[tT]o:")
      or string.match(newurl, "^vine:")
      or string.match(newurl, "^android%-app:")
      or string.match(newurl, "^ios%-app:")
      or string.match(newurl, "^data:")
      or string.match(newurl, "^irc:")
      or string.match(newurl, "^%${")
    ) then
      check(urlparse.absolute(url, newurl))
    end
  end

  local function check_new_params(newurl, param, value)
    if string.match(newurl, "[%?&]" .. param .. "=") then
      newurl = string.gsub(newurl, "([%?&]" .. param .. "=)[^%?&;]+", "%1" .. value)
    else
      if string.match(newurl, "%?") then
        newurl = newurl .. "&"
      else
        newurl = newurl .. "?"
      end
      newurl = newurl .. param .. "=" .. value
    end
    check(newurl)
  end

  local function increment_param(newurl, param, default, step)
    local value = string.match(newurl, "[%?&]" .. param .. "=([0-9]+)")
    if value then
      value = tonumber(value)
      value = value + step
      return check_new_params(newurl, param, tostring(value), 0)
    else
      return check_new_params(newurl, param, default)
    end
  end

  local function flatten_json(json)
    local result = ""
    for k, v in pairs(json) do
      result = result .. " " .. k
      local type_v = type(v)
      if type_v == "string" then
        v = string.gsub(v, "\\", "")
        result = result .. " " .. v .. ' "' .. v .. '"'
      elseif type_v == "table" then
        result = result .. " " .. flatten_json(v)
      end
    end
    return result
  end

  local function extract_from_json(json)
    local type_ = json["type"]
    local mdkey = json["mdkey"]
    if type_ and mdkey then
      check("https://www.vbox7.com/" .. type_ .. ":" .. mdkey)
    end
    for k, v in pairs(json) do
      if type(v) == "table" then
        extract_from_json(v)
      else
        if k == "video_uploader"
          or k == "uploader"
          or k == "contUploader" then
          check("https://www.vbox7.com/user:" .. v)
        elseif k == "vid"
          or k == "video_mdkey"
          or (
            k == "mdkey"
            and not json["type"]
          )
          or k == "contMdkey" then
          check("https://www.vbox7.com/play:" .. v)
        elseif k == "subtitleId" then
          check("https://www.vbox7.com/castsub/" .. v .. ".vtt")
        elseif k == "video_subtitles_id" then
          check("https://www.vbox7.com/castsub/" .. tostring(v) .. ".vtt")
        end
      end
    end
  end

  if allowed(url)
    and status_code < 300
    and not string.match(url, "^https?://i[0-9]+%.vbox7%.com/")
    and not string.match(url, "%.mp4$") then
    html = read_file(file)
    if string.match(url, "/aj/player/item/options%?vid=") then
      check("https://www.vbox7.com/play:" .. item_value)
      check("https://www.vbox7.com/aj/item/related?vid=" .. item_value)
      check("https://www.vbox7.com/aj/item/preview?mdkey=" .. item_value)
      check("https://www.vbox7.com/emb/external.php?vid=" .. item_value)
      check("https://www.vbox7.com/emb/external.php?vid=" .. item_value .. "&autoplay=0")
      check("https://www.vbox7.com/emb/external.php?vid=" .. item_value .. "&autoplay=1")
      check("https://i49.vbox7.com/player/ext.swf?vid=" .. item_value)
      check("https://i49.vbox7.com/player/ext.swf?vid=" .. item_value .. "&autoplay=0")
      check("https://i49.vbox7.com/player/ext.swf?vid=" .. item_value .. "&autoplay=1")
      check("https://api.vbox7.com/v4/?action=r_video_play&video_md5=" .. item_value .. "&app_token=imperia_android_0.0.7_4yxmKd")
    end
    if item_type == "subtitle" then
      check("https://i49.vbox7.com/subtitles/" .. string.match(item_value, "(...)$") .. "/" .. item_value .. "_2.js")
    end
    if item_type == "article" or item_type == "quiz" or item_type == "play" then
      check("https://www.vbox7.com/aj/comments?mdkey=" .. item_value .. "&page=1&order=")
    end
    if string.match(url, "%.mpd$") then
      for newurl in string.gmatch(html, '[uU][rR][lL]="([^"]+)"') do
        check(urlparse.absolute(url, newurl))
      end
      for adaptation_set in string.gmatch(html, "(<AdaptationSet.-</AdaptationSet>)") do
        local bandwidth = 0
        local newurl = nil
        for representation in string.gmatch(adaptation_set, "(<Representation.-</Representation>)") do
          local base_url = string.match(representation, "<BaseURL>([^<]+)</BaseURL>")
          local temp_bandwidth = tonumber(string.match(representation, 'bandwidth="([0-9]+)"'))
          if not newurl or temp_bandwidth > bandwidth then
            newurl = base_url
            bandwidth = temp_bandwidth
          end
        end
        if not newurl then
          error("Could not find URL in adaptation set.")
        end
        ids[newurl] = True
        check(urlparse.absolute(url, newurl))
      end
    end
    if string.match(url, "%?action=r_video_play") then
      json = cjson.decode(html)
      for _, data in pairs(json["items"]) do
        for _, category in pairs(data["video_categories"]) do
          check("https://www.vbox7.com/tag:" .. percent_encode_url(category))
        end
        for _, tag in pairs(data["video_tags"]) do
          check("https://www.vbox7.com/tag:" .. percent_encode_url(tag))
        end
        data["video_location"] = nil
        data["video_location_fallback"] = nil
        local newurl = data["video_subtitles_path"]
        if newurl then
          ids[newurl] = true
          check(newurl)
        end
        html = cjson.encode(json)
      end
    end
    if string.match(url, "/aj/comments") then
      json = cjson.decode(html)
      if string.len(json["html"]) > 0 then
        increment_param(url, "page", 0, 1)
      end
    end
    if string.match(html, "^%s*{") then
      if not json then
        json = cjson.decode(html)
      end
      extract_from_json(json)
      html = html .. flatten_json(json)
    end
    for newurl in string.gmatch(string.gsub(html, "&quot;", '"'), '([^"]+)') do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(string.gsub(html, "&#039;", "'"), "([^']+)") do
      checknewurl(newurl)
    end
    for newurl in string.gmatch(html, "[^%-]href='([^']+)'") do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, '[^%-]href="([^"]+)"') do
      checknewshorturl(newurl)
    end
    for newurl in string.gmatch(html, ":%s*url%(([^%)]+)%)") do
      checknewurl(newurl)
    end
    if not string.match(url, "%.mpd$") then
      html = string.gsub(html, "&gt;", ">")
      html = string.gsub(html, "&lt;", "<")
      for newurl in string.gmatch(html, ">%s*([^<%s]+)") do
        checknewurl(newurl)
      end
    end
  end

  return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
  status_code = http_stat["statcode"]
  set_item(url["url"])
  url_count = url_count + 1
  io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
  io.stdout:flush()
  logged_response = true
  if not item_name then
    error("No item name found.")
  end
  is_initial_url = false
  if string.match(url["url"], "^https?://[^/]+/aj/")
    or string.match(url["url"], "^https?://api%.vbox7%.com/") then
    local html = read_file(http_stat["local_file"])
    if not (
        string.match(html, "^%s*{")
        and string.match(html, "}%s*$")
      )
      and not (
        string.match(html, "^%s*%[")
        and string.match(html, "%]%s*$")
      ) then
      print("Did not get JSON data.")
      retry_url = true
      return false
    end
    local json = cjson.decode(html)
    if (
        string.match(url["url"], "/aj/")
        and not string.match(url["url"], "/item/preview")
        and not json["success"]
      )
      or (
        string.match(url["url"], "/player/item/options%?vid=")
        and not string.match(json["options"]["src"], item_value)
      ) then
      retry_url = true
      return false
    end
  end
  if http_stat["statcode"] ~= 200 then
    retry_url = true
    return false
  end
  if abortgrab then
    print("Not writing to WARC.")
    return false
  end
  retry_url = false
  tries = 0
  return true
end

wget.callbacks.httploop_result = function(url, err, http_stat)
  status_code = http_stat["statcode"]
  
  if not logged_response then
    url_count = url_count + 1
    io.stdout:write(url_count .. "=" .. status_code .. " " .. url["url"] .. " \n")
    io.stdout:flush()
  end
  logged_response = false

  if killgrab then
    return wget.actions.ABORT
  end

  set_item(url["url"])
  if not item_name then
    error("No item name found.")
  end

  if status_code >= 300 and status_code <= 399 then
    local newloc = urlparse.absolute(url["url"], http_stat["newloc"])
    if processed(newloc) or not allowed(newloc, url["url"]) then
      tries = 0
      return wget.actions.EXIT
    end
  end

  if abortgrab then
    abort_item()
    return wget.actions.EXIT
  end

  if status_code == 0 or retry_url then
    io.stdout:write("Server returned bad response. ")
    io.stdout:flush()
    tries = tries + 1
    local maxtries = 3
    if tries > maxtries then
      io.stdout:write(" Skipping.\n")
      io.stdout:flush()
      tries = 0
      abort_item()
      return wget.actions.EXIT
    end
    local sleep_time = math.random(
      math.floor(math.pow(2, tries-0.5)),
      math.floor(math.pow(2, tries))
    )
    io.stdout:write("Sleeping " .. sleep_time .. " seconds.\n")
    io.stdout:flush()
    os.execute("sleep " .. sleep_time)
    return wget.actions.CONTINUE
  else
    downloaded[url["url"]] = true
  end

  tries = 0

  return wget.actions.NOTHING
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
  local function submit_backfeed(items, key)
    local tries = 0
    local maxtries = 5
    while tries < maxtries do
      if killgrab then
        return false
      end
      local body, code, headers, status = http.request(
        "https://legacy-api.arpa.li/backfeed/legacy/" .. key,
        items .. "\0"
      )
      if code == 200 and body ~= nil and cjson.decode(body)["status_code"] == 200 then
        io.stdout:write(string.match(body, "^(.-)%s*$") .. "\n")
        io.stdout:flush()
        return nil
      end
      io.stdout:write("Failed to submit discovered URLs." .. tostring(code) .. tostring(body) .. "\n")
      io.stdout:flush()
      os.execute("sleep " .. math.floor(math.pow(2, tries)))
      tries = tries + 1
    end
    kill_grab()
    error()
  end

  local file = io.open(item_dir .. "/" .. warc_file_base .. "_bad-items.txt", "w")
  for url, _ in pairs(bad_items) do
    file:write(url .. "\n")
  end
  file:close()
  for key, data in pairs({
    ["vbox7-syoz7o6xfoctrj7m"] = discovered_items,
    ["urls-u7uag6wji741gqj0"] = discovered_outlinks
  }) do
    print('queuing for', string.match(key, "^(.+)%-"))
    local items = nil
    local count = 0
    for item, _ in pairs(data) do
      print("found item", item)
      if items == nil then
        items = item
      else
        items = items .. "\0" .. item
      end
      count = count + 1
      if count == 100 then
        submit_backfeed(items, key)
        items = nil
        count = 0
      end
    end
    if items ~= nil then
      submit_backfeed(items, key)
    end
  end
end

wget.callbacks.before_exit = function(exit_status, exit_status_string)
  if killgrab then
    return wget.exits.IO_FAIL
  end
  if abortgrab then
    abort_item()
  end
  return exit_status
end


