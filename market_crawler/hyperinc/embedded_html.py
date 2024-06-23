css = """
<style>
      html, body, div, dl, dt, dd, ul, ol, li, h1, h2, h3, h4, h5, h6, pre, code, form, fieldset, legend, input, textarea, p, blockquote, th, td, img {
        margin: 0;
        padding: 0;
      }
      table {
        width: 100%;
        border: 0;
        border-spacing: 0;
        border-collapse: collapse;
      }
      table {
        width: 100%;
        border: 0;
        border-spacing: 0;
        border-collapse: collapse;
      }
      th, td {
        border: 0;
        vertical-align: top;
      }
      li {
        list-style: none;
      }
      li {
        list-style: none;
      }
      img, fieldset {
        border: none;
        vertical-align: top;
      }
      .edibot-product-detail * {
        font-family: inherit;
        font-size: inherit;
      }
      #prdDetail img {
        max-width: 100% !important;
        height: auto !important;
      }
      #prdDetail img {
        max-width: 100% !important;
        height: auto !important;
      }
      #edinfo-studio div#edinfo-container, #edinfo-studio div#edinfo-container * {
        margin: 0;
        padding: 0;
        border: 0;
        -webkit-tap-highlight-color: rgba(0, 0, 0, 0);
        transform: none;
        transition: none;
        min-height: auto;
        height: auto;
        position: unset;
        box-sizing: content-box;
      }
      #edinfo-studio div#edinfo-container {
        min-width: 320px !important;
        max-width: 800px !important;
        margin: 0 auto !important;
        padding: 20px 0 !important;
        font-family: "malgun Gothic", arial, sans-serif;
        font-size: 12px;
        line-height: 1.2;
        letter-spacing: -1px;
        color: #242424;
      }
      #edinfo-studio {
        width: 100% !important;
        padding: 0 !important;
        margin: 0 !important;
      }
      #prdDetail {
        width: 960px;
        float: left;
        border-right: 1px solid #e5e5e5;
        padding: 60px 0 30px;
        overflow: hidden;
      }
      .xans-product-detail {
        position: relative;
        margin: 0 auto;
        padding: 30px 0 0 0;
      }
      .cboth {
        clear: both;
        *zoom:1: ;
      }
      .cboth::after {
        content: " ";
        display: block;
        clear: both;
      }
      #contents .xans-product-detail {
        width: 1470px;
        margin: 0 auto;
        padding: 0;
        position: static;
      }
      #contents {
        float: right;
        width: 1200px;
        min-height: 800px;
      }
      #header .inner, #footer .inner, #container, body.center .centerCategory .wyGrid, body.center .promotionArea .wyGrid {
        padding: 0;
      }
      #container, #contents {
        width: 100% !important;
      }
      #container {
        width: 1200px;
        margin: 0 auto;
        *zoom:1: ;
        position: relative;
      }
      #container::after {
        content: "";
        display: block;
        clear: both;
      }
      #wrap {
        position: relative;
        width: 100%;
        margin: 0 auto;
        background-color: #fff;
      }
      body, code {
        font: 0.75em Verdana, Dotum, AppleGothic, sans-serif;
        color: #353535;
        background: #fff;
      }
      body {
        min-width: 1480px;
      }
      body, code {
        font: 0.75em "Poppins", "Noto Sans KR", Verdana, Dotum, AppleGothic, sans-serif;
        color: #111;
        background: #fff;
      }
      body {
        min-width: 1240px;
      }
      .btn_moreview html, body {
        height: 100%;
      }
      body {
        min-width: 1470px;
      }
      html {
        width: 100%;
        height: 100%;
      }
      html {
        overflow-y: scroll;
      }
      #edinfo-studio div#edinfo-container .edinfo-title {
        color: #303030;
        font: bold 15pt "malgun Gothic", arial, sans-serif;
        text-align: center;
        padding: 40px 0 0;
        margin: 0;
      }
      #edinfo-studio div#edinfo-container .edinfo-board.edinfo-boardWear {
        position: relative;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail {
        position: relative;
      }
      #edinfo-studio div#edinfo-container > div:not(:first-child).edinfo-sizediv, #edinfo-studio div#edinfo-container > div:not(:first-child).edinfo-boardWear, #edinfo-studio div#edinfo-container > div:not(:first-child).edinfo-detail {
        margin-top: 40px;
      }
      #edinfo-studio div#edinfo-container .edinfo-board {
        background: #fff;
      }
      #edinfo-studio div#edinfo-container table {
        table-layout: fixed !important;
        width: 100% !important;
        border: 0;
        border-spacing: 0 !important;
        border-collapse: collapse !important;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail .edinfo-info {
        border-top: 1px solid #cccccc;
        border-bottom: 1px solid #cccccc;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail .sp-care-rows {
        margin-top: 0px;
        position: relative;
        border-bottom: 0;
      }
      #edinfo-studio div#edinfo-container th, #edinfo-studio div#edinfo-container td {
        word-wrap: break-word;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail .edinfo-info .sp-care-rows .sp-care-row-th {
        padding: 8px 5px 10px !important;
        width: 110px;
        background-color: #f9f9fa !important;
        vertical-align: top !important;
        text-align: right !important;
        font-weight: normal;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail .edinfo-info .sp-care-rows .sp-care-row-td {
        padding: 5px;
        position: relative;
      }
      #edinfo-studio div#edinfo-container ul, #edinfo-studio div#edinfo-container li {
        list-style: none !important;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail .edinfo-info tr .edinfo-value {
        overflow: hidden;
        line-height: 22px;
        flex: 1;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail .edinfo-info tr .edinfo-value .edinfo-desc {
        display: block;
        position: relative;
        padding: 2px 5px;
        border: 1px solid transparent;
        text-align: left;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail .edinfo-info tr .edinfo-name {
        line-height: 22px;
        padding: 0 9px;
        height: auto !important;
        white-space: pre-line;
        word-break: normal;
        display: flow-root;
        text-align: right;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail .edinfo-washing {
        margin: 0 0 12px 0;
        padding: 10px 0 0 5px;
        clear: both;
        text-align: left;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail .edinfo-washing li {
        display: inline-block;
        position: relative;
        max-width: 72px;
        width: 100%;
        margin: 4px 4px 0 0;
        text-align: center;
        vertical-align: top;
      }
      #edinfo-studio div#edinfo-container img {
        max-width: 60%;
        vertical-align: top;
      }
      #edinfo-studio div#edinfo-container table img {
        vertical-align: middle;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail .edinfo-washing li img {
        width: 40px;
        height: 40px;
      }
      #edinfo-studio div#edinfo-container .edinfo-detail .edinfo-washing li span {
        display: block;
        margin: 4px 0 12px;
        font-size: 11px;
      }
      #edinfo-studio div#edinfo-container .edinfo-board table {
        position: relative;
        border-width: 0 1px 1px 0;
        border-style: solid;
        border-color: #dadadc;
        font-size: 12px;
        text-align: left;
      }
      #edinfo-studio div#edinfo-container .edinfo-board table th {
        color: #303030;
        background: #f9f9fa;
      }
      #edinfo-studio div#edinfo-container .edinfo-board tbody th {
        border-width: 1px 0 0 1px;
        font-weight: bold;
        text-align: center;
      }
      #edinfo-studio div#edinfo-container .edinfo-board tbody th, #edinfo-studio div#edinfo-container .edinfo-board tbody td {
        overflow: hidden;
        min-height: 28px;
        padding: 5px 5px 6px;
        border: 1px solid #dadadc;
        line-height: 140%;
        text-align: center;
      }
      #edinfo-studio div#edinfo-container .edinfo-board tbody td {
        color: #303030;
      }
      #edinfo-studio div#edinfo-container .edinfo-board.edinfo-boardWear tbody td {
        vertical-align: middle;
        text-align: left;
      }
      #edinfo-studio div#edinfo-container .edinfo-board .edinfo-chk {
        position: relative;
        margin-left: 11%;
        width: auto;
      }
      #edinfo-studio div#edinfo-container .edinfo-board .edinfo-chk .edinfo-icoChk {
        display: inline-block;
        width: 11px;
        height: 11px;
        margin: 0 6% 0 0;
        border: 1px solid #d6d6d6;
        box-sizing: border-box;
        vertical-align: middle;
      }
      #edinfo-studio div#edinfo-container .edinfo-board .edinfo-chk .edinfo-icoChk-data {
        display: inline-block;
        vertical-align: middle;
        padding: 3px 0;
        width: 50%;
        box-sizing: border-box;
      }
      #edinfo-studio div#edinfo-container .edinfo-board .edinfo-chk.edinfo-checked .edinfo-icoChk {
        border: 1px solid transparent;
        background: #fb5555;
      }
      #edinfo-studio div#edinfo-container .edinfo-board table .edinfo-cell {
        display: block;
        width: 90%;
        position: relative;
        min-height: 16px;
        text-align: center;
        box-sizing: border-box;
        line-height: 26px;
      }
      #edinfo-studio div#edinfo-container .edinfo-text {
        position: relative;
        margin: 0;
        padding: 16px 10px;
        color: #636363;
        font-size: 12px;
        line-height: 1.5;
        white-space: pre-line;
        text-align: left;
        overflow-wrap: break-word;
      }
      #edinfo-studio div#edinfo-container .edinfo-text::before {
        display: none;
      }
      #edinfo-studio div#edinfo-container .edinfo-sizediv .edinfo-board {
        padding-top: 40px;
      }
      #edinfo-studio div#edinfo-container .edinfo-sizediv .edinfo-board > div {
        position: relative;
      }
      #edinfo-studio div#edinfo-container .edinfo-sizediv .edinfo-unit {
        position: absolute;
        right: 0px;
        top: -31px;
        padding: 6px 8px;
        border: 1px solid transparent;
        text-align: right;
      }
      #edinfo-studio div#edinfo-container .edinfo-sizediv .edinfo-board tbody th {
        background: #f9f9fa;
      }
      #edinfo-studio div#edinfo-container .edinfo-board thead th {
        padding: 5px 5px 6px;
        border-width: 1px 0 0 1px;
        border-style: solid;
        border-color: #dadadc;
        font-weight: bold;
        vertical-align: middle;
        line-height: 120%;
        text-align: center;
      }
    </style>
"""

html = """
    <div id="wrap">
      <div id="container">
        <div id="contents">
          <div class="xans-element- xans-product xans-product-detail cboth">
            <div id="prdDetail">
              <div class="cont">
                <div
                  class="edibot-product-detail"
                  style="margin: 0px auto; width: 1000px; max-width: 100%"
                >
                  <div class="edb-img-tag-w" style="position: relative">
                    <div style="margin: 0px auto">
                      <div id="edinfo-studio">"""
