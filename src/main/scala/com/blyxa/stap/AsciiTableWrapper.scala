package com.blyxa.stap

import de.vandermeer.asciitable.AsciiTable
import de.vandermeer.skb.interfaces.transformers.textformat.TextAlignment
import org.fusesource.jansi.AnsiConsole
import org.jline.terminal.TerminalBuilder

object AsciiTableWrapper{
  // Ansi color init
  AnsiConsole.systemInstall()

  final val TERMINAL_WIDTH = TerminalBuilder.terminal().getWidth
  final val HEADER_PADDING = TERMINAL_WIDTH

  def newTable(hCols:Array[String]): AsciiTable ={
    val table = new AsciiTableWrapper()
    table.getContext.setWidth(TERMINAL_WIDTH)
    table.addRule()
    table.addRow(hCols:_*)
    table.addRule()
    table
  }
}
class AsciiTableWrapper extends AsciiTable{
  override def render(): String ={
    setTextAlignment(TextAlignment.LEFT)
    super.render()
  }
}