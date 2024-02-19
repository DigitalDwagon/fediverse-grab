local urlparse = require("socket.url")
local http = require("socket.http")
local https = require("ssl.https")
local cjson = require("cjson")
local utf8 = require("utf8")

local item_dir = os.getenv("item_dir")
local warc_file_base = os.getenv("warc_file_base")
local concurrency = tonumber(os.getenv("concurrency"))

local item_type = nil
local item_host = nil
local item_value = nil

local discovered = {}


local function discover_item(type, value)
    -- get host from value
    local host = urlparse.parse(value).host
    if host == nil then
        return
    end
    --print("discovered " .. type .. ":" .. host .. ":" .. value)
    table.insert(discovered, { ["type"]=type, ["host"]=host, ["value"]=value })

end

local function find_item(url)
    local itemType, itemHost, itemValue = nil, nil, nil
    if url:find("https://archiveteam%-items%.invalid/") then
        itemType, itemHost, itemValue = url:match("https://archiveteam%-items%.invalid/(.-):(.-):(https?://.+)")
    end
    return {
        ["type"] = itemType,
        ["host"] = itemHost,
        ["value"] = itemValue
    }
end



wget.callbacks.download_child_p = function(urlpos, parent, depth, start_url_parsed, iri, verdict, reason)
    local url = urlpos["url"]["url"]
    print(url .. " download_child_p")
    if item_type == nil then
        item_type, item_host, item_value = find_item(url)
    end

    return false
end

local url_count = 0

wget.callbacks.httploop_result = function(url, err, http_stat)
    print(url["url"] .. " httploop_result")
    if item_type == nil then
        local item_info = find_item(url["url"])
        print(item_info)
        item_type = item_info["type"]
        item_host = item_info["host"]
        item_value = item_info["value"]
    end

    status_code = http_stat["statcode"]

    if not logged_response then
        url_count = url_count + 1
        print(url_count .. "=" .. status_code .. " " .. url["url"] )
    end
    os.execute("sleep 0.1")
    return wget.actions.NOTHING
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


local function discover_items_from_post_json(json)
    if json["id"] ~= nil and json["id"] ~= item_value then
        discover_item("post", json["id"])
    end
    --[[
     handle case   "inReplyTo": null
    ]]
    if type(json["inReplyTo"]) == "string" then
        discover_item("post", json["inReplyTo"])
    end
    if json["url"] ~= nil then
        discover_item("ipost", json["url"])
    end
    if json["attributedTo"] ~= nil then
        discover_item("user", json["attributedTo"])
    end
    --[[if json["cc"] ~= nil then
        for i, cc in ipairs(json["cc"]) do
            discover_item("user", cc)
        end
    end]]
    if json["attachment"] ~= nil then
        for i, attachment in ipairs(json["attachment"]) do
            if attachment["type"] == "Document" then
                discover_item("media", attachment["url"])
            end
        end
    end
    if json["tag"] ~= nil then
        for i, tag in ipairs(json["tag"]) do
            if tag["type"] == "Mention" then
                discover_item("user", tag["href"])
            end
        end
    end
    if json["replies"] ~= nil then
        if json["replies"]["id"] ~= nil then
            discover_item("collection", json["replies"]["id"])
        end
    end
end

wget.callbacks.get_urls = function(file, url, is_css, iri)
    print(url .. " get_urls")
    local urls = {}
    if url:find("https://archiveteam%-items%.invalid/") then
        if item_type == "post" then
            print("would queue " .. item_value)
            table.insert(urls, { url=item_value, headers={ ["Accept"]="application/ld+json; profile=\"https://www.w3.org/ns/activitystreams" } })
        end
        if item_type == "collection" then
            print("would queue " .. item_value)
            table.insert(urls, { url=item_value, headers={ ["Accept"]="application/ld+json; profile=\"https://www.w3.org/ns/activitystreams" } })
        end
    end
    -- Check if the file is valid JSON.
    local status, json = pcall(cjson.decode, read_file(file))
    if status then
        print("valid JSON")
        if item_type == "post" then
            discover_items_from_post_json(json)
        end

        if item_type == "collection" then
            --[[
            {
              "@context": "https://www.w3.org/ns/activitystreams",
              "id": "https://digipres.club/users/textfiles/statuses/111949542506351689/replies",
              "type": "Collection",
              "first": {
                "id": "https://digipres.club/users/textfiles/statuses/111949542506351689/replies?page=true",
                "type": "CollectionPage",
                "next": "https://digipres.club/users/textfiles/statuses/111949542506351689/replies?only_other_accounts=true&page=true",
                "partOf": "https://digipres.club/users/textfiles/statuses/111949542506351689/replies",
                "items": []
              }
            }
            ]]
            if json["type"] ~= nil and json["type"] == "Collection" then
                if json["first"] ~= nil then
                    if json["first"]["id"] ~= nil then
                        table.insert(urls, { url=json["first"]["id"], headers={ ["Accept"]="application/ld+json; profile=\"https://www.w3.org/ns/activitystreams" } })
                    end
                    if json["first"]["next"] ~= nil then
                        table.insert(urls, { url=json["first"]["next"], headers={ ["Accept"]="application/ld+json; profile=\"https://www.w3.org/ns/activitystreams" } })
                    end
                    if json["first"]["items"] ~= nil then
                        for i, item in ipairs(json["first"]["items"]) do
                            if item["id"] ~= nil then
                                discover_item("post", item["id"])
                            end
                        end
                    end
                end
            end

            if json["type"] ~= nil and json["type"] == "CollectionPage" then
                if json["next"] ~= nil then
                    table.insert(urls, { url=json["next"], headers={ ["Accept"]="application/ld+json; profile=\"https://www.w3.org/ns/activitystreams" } })
                end
                if json["items"] ~= nil then
                    for i, item in ipairs(json["items"]) do
                        -- if it's just a string queue it as the post id, otherwise get the id from the object
                        if type(item) == "string" then
                            discover_item("post", item)

                        else
                            if item["id"] ~= nil then
                                discover_item("post", item["id"])
                                discover_items_from_post_json(item)
                            end
                        end
                    end
                end
            end
        end
    end
    return urls
end

wget.callbacks.write_to_warc = function(url, http_stat)
    status_code = http_stat["statcode"]
    if status_code == 200 then
        return true
    end
    return false
end

wget.callbacks.finish = function(start_time, end_time, wall_time, numurls, total_downloaded_bytes, total_download_time)
    print("finish")
    -- print discovered items from discovered table to stdout
    for i, item in ipairs(discovered) do
        print(item["type"] .. ":" .. item["host"] .. ":" .. item["value"])
    end
end