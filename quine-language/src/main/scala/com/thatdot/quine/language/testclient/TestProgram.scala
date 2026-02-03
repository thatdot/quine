//package com.quine.language.testclient
//
//import com.quine.language.server.QuineLanguageServer
//import org.eclipse.lsp4j.{CompletionParams, Position, TextDocumentIdentifier}
//import org.eclipse.lsp4j.launch.LSPLauncher
//
//import java.io.{PipedInputStream, PipedOutputStream}
//
//object TestProgram {
//  def main(args: Array[String]): Unit = {
//    val inClient = new PipedInputStream
//    val outClient = new PipedOutputStream
//    val inServer = new PipedInputStream
//    val outServer = new PipedOutputStream
//
//    inClient.connect(outServer)
//    outClient.connect(inServer)
//
//    val server = new QuineLanguageServer
//    val serverLauncher = LSPLauncher.createServerLauncher(server, inServer, outServer)
//    val serverListening = serverLauncher.startListening
//
//    val client = new QuineLanguageClient
//    val clientLauncher = LSPLauncher.createClientLauncher(client, inClient, outClient)
//    val clientListening = clientLauncher.startListening
//
//    val p = new CompletionParams
//    p.setPosition(new Position(1, 1))
//    p.setTextDocument(new TextDocumentIdentifier("data/query1.quine"))
//
//    val future = clientLauncher.getRemoteProxy.getTextDocumentService.completion(p)
//
//    println(future.join())
//  }
//}
