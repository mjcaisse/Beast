//
// Copyright (c) 2013-2017 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef BEAST_EXAMPLES_WEBSOCKET_ASYNC_ECHO_SERVER_HPP
#define BEAST_EXAMPLES_WEBSOCKET_ASYNC_ECHO_SERVER_HPP

#include "server.hpp"

#include <beast/core/multi_buffer.hpp>
#include <beast/websocket/stream.hpp>
#include <atomic>
#include <functional>
#include <memory>
#include <ostream>
#include <type_traits>
#include <utility>

/** Asynchronous WebSocket echo client/server
*/
class websocket_echo_async_server
    : public server<websocket_echo_async_server>
{
    using endpoint_type =
        boost::asio::ip::tcp::endpoint;

    using on_new_stream_cb  =
        std::function<void(beast::websocket::stream<socket_type>&)>;

    class connection;

    std::ostream& log_;
    on_new_stream_cb cb_;

public:
    websocket_echo_async_server(websocket_echo_async_server const&) = delete;
    websocket_echo_async_server& operator=(websocket_echo_async_server const&) = delete;

    /** Constructor

        @param log A stream to log to.

        @param args Optional arguments forwarded to the @ref server
        constructor.
    */
    template<class... Args>
    explicit
    websocket_echo_async_server(
            std::ostream& log, Args&&... args)
        : server<websocket_echo_async_server>(
            std::forward<Args>(args)...)
        , log_(log)
    {
    }

    /** Set a handler called for new streams.

        This function is called for each new stream. It
        is used to set options for every WebSocket connection.
    */
    void
    on_new_stream(on_new_stream_cb f)
    {
        cb_ = std::move(f);
    }

private:
    friend class server<websocket_echo_async_server>;

    class connection
        : public std::enable_shared_from_this<connection>
    {
        int state = 0;
        websocket_echo_async_server& server_;
        endpoint_type ep_;
        beast::websocket::stream<socket_type> ws_;
        boost::asio::io_service::strand strand_;
        beast::multi_buffer buffer_;
        std::size_t id_;

    public:
        connection(connection&&) = default;
        connection(connection const&) = default;
        connection& operator=(connection&&) = delete;
        connection& operator=(connection const&) = delete;

        template<class... Args>
        explicit
        connection(websocket_echo_async_server& server,
                endpoint_type const& ep, socket_type&& sock)
            : server_(server)
            , ep_(ep)
            , ws_(std::move(sock))
            , strand_(ws_.get_io_service())
            , id_([]
                {
                    static std::atomic<std::size_t> n{0};
                    return ++n;
                }())
        {
            server_.cb_(ws_);
        }

        void
        run()
        {
            ws_.async_accept_ex(
                [](beast::websocket::response_type& res)
                {
                    res.insert(
                        "Server", "async_echo_server");
                },
                strand_.wrap(std::bind(&connection::on_accept,
                    shared_from_this(), std::placeholders::_1)));
        }

        void
        on_accept(error_code ec)
        {
            if(ec)
                return fail("async_accept", ec);
            do_read();
        }

        void
        do_read()
        {
            ws_.async_read(buffer_, strand_.wrap(
                std::bind(&connection::on_read, shared_from_this(),
                    std::placeholders::_1)));
        }
        
        void
        on_read(error_code ec)
        {
            if(ec == beast::websocket::error::closed)
                return;

            if(ec)
                return fail("on_read", ec);

            ws_.binary(ws_.got_binary());

            ws_.async_write(buffer_.data(), strand_.wrap(
                std::bind(&connection::on_write, shared_from_this(),
                    std::placeholders::_1)));
        }

        void
        on_write(error_code ec)
        {
            if(ec)
                return fail("on_write", ec);

            buffer_.consume(buffer_.size());

            do_read();
        }

    private:
        void
        fail(std::string what, error_code ec)
        {
            if(ec != beast::websocket::error::closed)
                server_.log_ <<
                    "[#" << id_ << " " << ep_ << "] " <<
                what << ": " << ec.message() << std::endl;
        }
    };

    void
    do_accept(socket_type&& sock, endpoint_type ep)
    {
        std::make_shared<connection>(*this, ep, std::move(sock))->run();
    }
};

#endif
