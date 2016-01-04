local socket = require("socket")

local host = "175.10.100.161"
local port = 8111
local script = "main.lua"

local eventList = { "recv", "send", "timeout" }

-- global functions

function sock_connect(sock, address, port, timeout)

	sock:settimeout(0)			
	sock:connect(address, port)

end

function sock_recv(sock, n, timeout)

	sock:settimeout(0)

	local time 
	
	if timeout then
		time = os.time() + timeout
	end

	while true do
		local e = coroutine.yield({ sock = sock, time = time, recv = true })

		if e.timeout then
			return nil, "timeout"
		end

		local ok, err = sock:receive(n)	

		if not ok and err ~= "timeout" then
			return nil, err
		elseif ok then
			return ok
		end
	end

end

function sock_send(sock, s, timeout)

	local time 

	if timeout then
		time = os.time() + timeout
	end

	while true do
		local e = coroutine.yield({ sock = sock, time = time, send = true })

		if e.timeout then
			return nil, "timeout"
		end

		local l, err = sock:send(s)

		if not l then
			return nil, err
		else
			if l < #s then
				s = s:sub(l + 1)							
			else
				return true
			end
		end
	end
end

function sleep(timeout)

	timeout = timeout or 1
	coroutine.yield({ time = os.time() + timeout })

end

local process = loadfile(script)

local function accept(sock, events)

	coroutine.yield({ sock = sock,  recv = true })

	while true do
		local newSock, err = sock:accept()		
		if newSock then
			local newCo = coroutine.create(process)
			_, events[newCo] = coroutine.resume(newCo, newSock)
		end

		coroutine.yield({ sock = sock, recv = true })
	end
end

local function scanArgs(events)

	local recvt = {}
	local sendt = {}
	local timeout = -1
	local sock2co = {}
	local time2co = {}

	local currentTime = os.time()
	local minWait = -1

	for co, e in pairs(events) do

		if e.sock then
			if e.recv then
				table.insert(recvt, e.sock)
			end
			if e.send then
				table.insert(sendt, e.sock)
			end
			sock2co[e.sock] = co
		end

		if e.time then
			if timeout == -1 then
				table.insert(time2co, co)
				minWait = e.time
			else
				local wait = e.time - currentTime		
		
				if wait < 0 then
					wait = 0
				end

				if wait < minWait then
					minWait = wait		
					time2co = {}
					table.insert(time2co, co)
				elseif wait == minWait then
					table.insert(time2co, co)
				end
			end
		end
	end

	if minWait ~= -1 then
		timeout = minWait - currentTime
	end

	return recvt, sendt, timeout, sock2co, time2co
end

local function getReadyCoroutines(recv, send, err, sock2co, time2co)

	local ready = {}	

	for _, sock in ipairs(recv) do
		local co = sock2co[sock]
		if not ready[co] then
			ready[co] = {}
		end
		ready[co].recv = true
	end

	for _, sock in ipairs(send) do
		local co = sock2co[sock]
		if not ready[co] then
			ready[co] = {}
		end
		ready[co].send = true
	end

	if err == "timeout" then
		for _, co in ipairs(time2co) do
			if not ready[co] then
				ready[co] = {}
			end
			ready[co].timeout = true
		end
	end

	return ready
end

local function executeCoroutines(ready, events)

	for co, args in pairs(ready) do
		local ok, event = coroutine.resume(co, args)

		if not ok then
			events[co] = nil
		elseif coroutine.status(co) == "dead" then
			i = i + 1
			print("close the thread ", i, co)
			events[co] = nil
		else
			events[co] = event
		end
	end

end

local events = {}

local server = socket.bind(host, port, 1024)
server:settimeout(0)
server:setoption("reuseaddr", true)

local acceptCo = coroutine.create(accept)
_, events[acceptCo] = coroutine.resume(acceptCo, server, events)

while true do

	-- 1. 收集要检测的事件
	local recvt, sendt, timeout, sock2co, time2co = scanArgs(events)
	
	-- 2. 检测事件
	local recv, send, err = socket.select(recvt, sendt, timeout)	

	-- 3. 获取等待事件已经好了的协程
	local ready = getReadyCoroutines(recv, send, err, sock2co, time2co)

	-- 4. 执行协程
	executeCoroutines(ready, events)		

	-- 5. 执行垃圾收集，回收资源
	collectgarbage()
end

