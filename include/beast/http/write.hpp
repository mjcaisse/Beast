//
// Copyright (c) 2013-2017 Vinnie Falco (vinnie dot falco at gmail dot com)
//
// Distributed under the Boost Software License, Version 1.0. (See accompanying
// file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)
//

#ifndef BEAST_HTTP_WRITE_HPP
#define BEAST_HTTP_WRITE_HPP

#include <beast/config.hpp>
#include <beast/core/buffer_cat.hpp>
#include <beast/core/consuming_buffers.hpp>
#include <beast/core/multi_buffer.hpp>
#include <beast/http/message.hpp>
#include <beast/http/serializer.hpp>
#include <beast/http/detail/chunk_encode.hpp>
#include <beast/core/error.hpp>
#include <beast/core/async_result.hpp>
#include <beast/core/string_view.hpp>
#include <boost/variant.hpp>
#include <iosfwd>
#include <limits>
#include <memory>
#include <type_traits>
#include <utility>

namespace beast {
namespace http {

/** Write part of a message to a stream using a serializer.

    This function is used to write part of a message to a stream using
    a caller-provided HTTP/1 serializer. The call will block until one
    of the following conditions is true:
        
    @li One or more bytes have been transferred.

    @li The function @ref serializer::is_done returns `true`

    @li An error occurs on the stream.

    This operation is implemented in terms of one or more calls
    to the stream's `write_some` function.

    The amount of data actually transferred is controlled by the behavior
    of the underlying stream, performing bounded work for each call. This
    helps applications set reasonable timeouts. It also allows application-level
    flow control to function correctly. For example when using a TCP/IP based
    stream.
    
    @param stream The stream to which the data is to be written.
    The type must support the @b SyncWriteStream concept.

    @param sr The serializer to use.

    @throws system_error Thrown on failure.

    @see serializer
*/
template<class SyncWriteStream, bool isRequest,
    class Body, class Fields, class Decorator>
void
write_some(SyncWriteStream& stream, serializer<
    isRequest, Body, Fields, Decorator>& sr);

/** Write part of a message to a stream using a serializer.

    This function is used to write part of a message to a stream using
    a caller-provided HTTP/1 serializer. The call will block until one
    of the following conditions is true:
        
    @li One or more bytes have been transferred.

    @li The function @ref serializer::is_done returns `true`

    @li An error occurs on the stream.

    This operation is implemented in terms of one or more calls
    to the stream's `write_some` function.

    The amount of data actually transferred is controlled by the behavior
    of the underlying stream, performing bounded work for each call. This
    helps applications set reasonable timeouts. It also allows application-level
    flow control to function correctly. For example when using a TCP/IP based
    stream.
    
    @param stream The stream to which the data is to be written.
    The type must support the @b SyncWriteStream concept.

    @param sr The serializer to use.

    @param ec Set to indicate what error occurred, if any.

    @see @ref async_write_some, @ref serializer
*/
template<class SyncWriteStream, bool isRequest,
    class Body, class Fields, class Decorator>
void
write_some(SyncWriteStream& stream, serializer<
    isRequest, Body, Fields, Decorator>& sr,
        error_code& ec);

/** Write part of a message to a stream asynchronously using a serializer.

    This function is used to write part of a message to a stream
    asynchronously using a caller-provided HTTP/1 serializer. The function
    call always returns immediately. The asynchronous operation will continue
    until one of the following conditions is true:

    @li One or more bytes have been transferred.

    @li The function @ref serializer::is_done returns `true`

    @li An error occurs on the stream.

    This operation is implemented in terms of zero or more calls to the stream's
    `async_write_some` function, and is known as a <em>composed operation</em>.
    The program must ensure that the stream performs no other write operations
    until this operation completes.

    The amount of data actually transferred is controlled by the behavior
    of the underlying stream, performing bounded work for each call. This
    helps applications set reasonable timeouts. It also allows application-level
    flow control to function correctly. For example when using a TCP/IP based
    stream.
    
    @param stream The stream to which the data is to be written.
    The type must support the @b AsyncWriteStream concept.

    @param sr The serializer to use.
    The object must remain valid at least until the
    handler is called; ownership is not transferred.

    @param handler The handler to be called when the operation
    completes. Copies will be made of the handler as required.
    The equivalent function signature of the handler must be:
    @code void handler(
        error_code const& error // result of operation
    ); @endcode
    Regardless of whether the asynchronous operation completes
    immediately or not, the handler will not be invoked from within
    this function. Invocation of the handler will be performed in a
    manner equivalent to using `boost::asio::io_service::post`.

    @see @ref serializer
*/
template<class AsyncWriteStream,
    bool isRequest, class Body, class Fields,
        class Decorator, class WriteHandler>
#if BEAST_DOXYGEN
    void_or_deduced
#else
async_return_type<WriteHandler, void(error_code)>
#endif
async_write_some(AsyncWriteStream& stream, serializer<
    isRequest, Body, Fields, Decorator>& sr,
        WriteHandler&& handler);

//------------------------------------------------------------------------------

/** Write a header to a stream using a serializer.

    This function is used to write a header to a stream using a
    caller-provided HTTP/1 serializer. The call will block until one
    of the following conditions is true:

    @li The function @ref serializer::is_header_done returns `true`

    @li An error occurs.

    This operation is implemented in terms of one or more calls
    to the stream's `write_some` function.

    @param stream The stream to which the data is to be written.
    The type must support the @b SyncWriteStream concept.

    @param sr The serializer to use.

    @throws system_error Thrown on failure.

    @note The implementation will call @ref serializer::split with
    the value `true` on the serializer passed in.

    @see @ref serializer
*/
template<class SyncWriteStream, bool isRequest,
    class Body, class Fields, class Decorator>
void
write_header(SyncWriteStream& stream, serializer<
    isRequest, Body, Fields, Decorator>& sr);

/** Write a header to a stream using a serializer.

    This function is used to write a header to a stream using a
    caller-provided HTTP/1 serializer. The call will block until one
    of the following conditions is true:

    @li The function @ref serializer::is_header_done returns `true`

    @li An error occurs.

    This operation is implemented in terms of one or more calls
    to the stream's `write_some` function.

    @param stream The stream to which the data is to be written.
    The type must support the @b SyncWriteStream concept.

    @param sr The serializer to use.

    @param ec Set to indicate what error occurred, if any.

    @note The implementation will call @ref serializer::split with
    the value `true` on the serializer passed in.

    @see @ref serializer
*/
template<class SyncWriteStream, bool isRequest,
    class Body, class Fields, class Decorator>
void
write_header(SyncWriteStream& stream, serializer<
    isRequest, Body, Fields, Decorator>& sr,
        error_code& ec);

/** Write a header to a stream asynchronously using a serializer.

    This function is used to write a header to a stream asynchronously
    using a caller-provided HTTP/1 serializer. The function call always
    returns immediately. The asynchronous operation will continue until
    one of the following conditions is true:

    @li The function @ref serializer::is_header_done returns `true`

    @li An error occurs.

    This operation is implemented in terms of zero or more calls to the stream's
    `async_write_some` function, and is known as a <em>composed operation</em>.
    The program must ensure that the stream performs no other write operations
    until this operation completes.

    @param stream The stream to which the data is to be written.
    The type must support the @b AsyncWriteStream concept.

    @param sr The serializer to use.
    The object must remain valid at least until the
    handler is called; ownership is not transferred.

    @param handler The handler to be called when the operation
    completes. Copies will be made of the handler as required.
    The equivalent function signature of the handler must be:
    @code void handler(
        error_code const& error // result of operation
    ); @endcode
    Regardless of whether the asynchronous operation completes
    immediately or not, the handler will not be invoked from within
    this function. Invocation of the handler will be performed in a
    manner equivalent to using `boost::asio::io_service::post`.

    @note The implementation will call @ref serializer::split with
    the value `true` on the serializer passed in.

    @see @ref serializer
*/
template<class AsyncWriteStream,
    bool isRequest, class Body, class Fields,
        class Decorator, class WriteHandler>
#if BEAST_DOXYGEN
    void_or_deduced
#else
async_return_type<WriteHandler, void(error_code)>
#endif
async_write_header(AsyncWriteStream& stream, serializer<
    isRequest, Body, Fields, Decorator>& sr,
        WriteHandler&& handler);

//------------------------------------------------------------------------------

/** Write a complete message to a stream using a serializer.

    This function is used to write a complete message to a stream using
    a caller-provided HTTP/1 serializer. The call will block until one
    of the following conditions is true:

    @li The function @ref serializer::is_done returns `true`

    @li An error occurs.

    This operation is implemented in terms of one or more calls
    to the stream's `write_some` function.

    @param stream The stream to which the data is to be written.
    The type must support the @b SyncWriteStream concept.

    @param sr The serializer to use.

    @throws system_error Thrown on failure.

    @see @ref serializer
*/
template<class SyncWriteStream, bool isRequest,
    class Body, class Fields, class Decorator>
void
write(SyncWriteStream& stream, serializer<
    isRequest, Body, Fields, Decorator>& sr);

/** Write a complete message to a stream using a serializer.

    This function is used to write a complete message to a stream using
    a caller-provided HTTP/1 serializer. The call will block until one
    of the following conditions is true:

    @li The function @ref serializer::is_done returns `true`

    @li An error occurs.

    This operation is implemented in terms of one or more calls
    to the stream's `write_some` function.

    @param stream The stream to which the data is to be written.
    The type must support the @b SyncWriteStream concept.

    @param sr The serializer to use.

    @param ec Set to the error, if any occurred.

    @see @ref serializer
*/
template<class SyncWriteStream, bool isRequest,
    class Body, class Fields, class Decorator>
void
write(SyncWriteStream& stream, serializer<
    isRequest, Body, Fields, Decorator>& sr,
        error_code& ec);

/** Write a complete message to a stream asynchronously using a serializer.

    This function is used to write a complete message to a stream
    asynchronously using a caller-provided HTTP/1 serializer. The
    function call always returns immediately. The asynchronous
    operation will continue until one of the following conditions is true:

    @li The function @ref serializer::is_done returns `true`

    @li An error occurs.

    This operation is implemented in terms of zero or more calls to the stream's
    `async_write_some` function, and is known as a <em>composed operation</em>.
    The program must ensure that the stream performs no other write operations
    until this operation completes.

    @param stream The stream to which the data is to be written.
    The type must support the @b AsyncWriteStream concept.

    @param sr The serializer to use.
    The object must remain valid at least until the
    handler is called; ownership is not transferred.

    @param handler The handler to be called when the operation
    completes. Copies will be made of the handler as required.
    The equivalent function signature of the handler must be:
    @code void handler(
        error_code const& error // result of operation
    ); @endcode
    Regardless of whether the asynchronous operation completes
    immediately or not, the handler will not be invoked from within
    this function. Invocation of the handler will be performed in a
    manner equivalent to using `boost::asio::io_service::post`.

    @see @ref serializer
*/
template<class AsyncWriteStream,
    bool isRequest, class Body, class Fields,
        class Decorator, class WriteHandler>
#if BEAST_DOXYGEN
    void_or_deduced
#else
async_return_type<WriteHandler, void(error_code)>
#endif
async_write(AsyncWriteStream& stream, serializer<
    isRequest, Body, Fields, Decorator>& sr,
        WriteHandler&& handler);

//------------------------------------------------------------------------------

/** Write a complete message to a stream.

    This function is used to write a complete message to a stream using
    HTTP/1. The call will block until one of the following conditions is true:

    @li The entire message is written.

    @li An error occurs.

    This operation is implemented in terms of one or more calls to the stream's
    `write_some` function. The algorithm will use a temporary @ref serializer
    with an empty chunk decorator to produce buffers. If the semantics of the
    message indicate that the connection should be closed after the message is
    sent, the error delivered by this function will be @ref error::end_of_stream

    @param stream The stream to which the data is to be written.
    The type must support the @b SyncWriteStream concept.

    @param msg The message to write.

    @throws system_error Thrown on failure.

    @see @ref message
*/
template<class SyncWriteStream,
    bool isRequest, class Body, class Fields>
void
write(SyncWriteStream& stream,
    message<isRequest, Body, Fields> const& msg);

/** Write a complete message to a stream.

    This function is used to write a complete message to a stream using
    HTTP/1. The call will block until one of the following conditions is true:

    @li The entire message is written.

    @li An error occurs.

    This operation is implemented in terms of one or more calls to the stream's
    `write_some` function. The algorithm will use a temporary @ref serializer
    with an empty chunk decorator to produce buffers. If the semantics of the
    message indicate that the connection should be closed after the message is
    sent, the error delivered by this function will be @ref error::end_of_stream

    @param stream The stream to which the data is to be written.
    The type must support the @b SyncWriteStream concept.

    @param msg The message to write.

    @param ec Set to the error, if any occurred.

    @see @ref message
*/
template<class SyncWriteStream,
    bool isRequest, class Body, class Fields>
void
write(SyncWriteStream& stream,
    message<isRequest, Body, Fields> const& msg,
        error_code& ec);

/** Write a complete message to a stream asynchronously.

    This function is used to write a complete message to a stream asynchronously
    using HTTP/1. The function call always returns immediately. The asynchronous
    operation will continue until one of the following conditions is true:

    @li The entire message is written.

    @li An error occurs.

    This operation is implemented in terms of zero or more calls to the stream's
    `async_write_some` function, and is known as a <em>composed operation</em>.
    The program must ensure that the stream performs no other write operations
    until this operation completes. The algorithm will use a temporary
    @ref serializer with an empty chunk decorator to produce buffers. If
    the semantics of the message indicate that the connection should be
    closed after the message is sent, the error delivered by this function
    will be @ref error::end_of_stream

    @param stream The stream to which the data is to be written.
    The type must support the @b AsyncWriteStream concept.

    @param msg The message to write.
    The object must remain valid at least until the
    handler is called; ownership is not transferred.

    @param handler The handler to be called when the operation
    completes. Copies will be made of the handler as required.
    The equivalent function signature of the handler must be:
    @code void handler(
        error_code const& error // result of operation
    ); @endcode
    Regardless of whether the asynchronous operation completes
    immediately or not, the handler will not be invoked from within
    this function. Invocation of the handler will be performed in a
    manner equivalent to using `boost::asio::io_service::post`.

    @see @ref message
*/
template<class AsyncWriteStream,
    bool isRequest, class Body, class Fields,
        class WriteHandler>
async_return_type<
    WriteHandler, void(error_code)>
async_write(AsyncWriteStream& stream,
    message<isRequest, Body, Fields> const& msg,
        WriteHandler&& handler);

//------------------------------------------------------------------------------

/** Serialize an HTTP/1 header to a `std::ostream`.

    The function converts the header to its HTTP/1 serialized
    representation and stores the result in the output stream.

    @param os The output stream to write to.

    @param msg The message fields to write.
*/
template<bool isRequest, class Fields>
std::ostream&
operator<<(std::ostream& os,
    header<isRequest, Fields> const& msg);

/** Serialize an HTTP/1 message to a `std::ostream`.

    The function converts the message to its HTTP/1 serialized
    representation and stores the result in the output stream.

    The implementation will automatically perform chunk encoding if
    the contents of the message indicate that chunk encoding is required.

    @param os The output stream to write to.

    @param msg The message to write.
*/
template<bool isRequest, class Body, class Fields>
std::ostream&
operator<<(std::ostream& os,
    message<isRequest, Body, Fields> const& msg);

} // http
} // beast

#include <beast/http/impl/write.ipp>

#endif
