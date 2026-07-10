*** Begin Patch
*** Update File: internal/server/proxy.go
@@
-    var up upstream
-    if wsMode {
-        upURL, uerr := s.buildUpstreamURL(r)
-        if uerr != nil {
-            log.Printf("server: bad upstream url: %v", uerr)
-            return
-        }
-        up = NewResilientWSUpstream(s.ctx, upURL, websocket.Subprotocols(r), s.cfg.UpstreamTimeout, s.cfg.ReconnectTimeout)
-    } else {
-        up = NewResilientUpstream(s.ctx, s.cfg.UpstreamAddr, s.cfg.UpstreamTimeout, s.cfg.ReconnectTimeout)
-    }
+    var up upstream
+    if wsMode {
+        upURL, uerr := s.buildUpstreamURL(r)
+        if uerr != nil {
+            log.Printf("server: bad upstream url: %v", uerr)
+            return
+        }
+        // Build X-Forwarded-* headers (append semantics)
+        hdr := http.Header{}
+        if prior := r.Header.Get("X-Forwarded-Host"); prior != "" {
+            hdr.Set("X-Forwarded-Host", prior+", "+r.Host)
+        } else {
+            hdr.Set("X-Forwarded-Host", r.Host)
+        }
+        if ip, _, err := net.SplitHostPort(r.RemoteAddr); err == nil {
+            if prior := r.Header.Get("X-Forwarded-For"); prior != "" {
+                hdr.Set("X-Forwarded-For", prior+", "+ip)
+            } else {
+                hdr.Set("X-Forwarded-For", ip)
+            }
+        }
+        if r.TLS != nil {
+            hdr.Set("X-Forwarded-Proto", "https")
+        } else {
+            hdr.Set("X-Forwarded-Proto", "http")
+        }
+
+        up = NewResilientWSUpstream(s.ctx, upURL, websocket.Subprotocols(r), hdr, s.cfg.UpstreamTimeout, s.cfg.ReconnectTimeout)
+    } else {
+        up = NewResilientUpstream(s.ctx, s.cfg.UpstreamAddr, s.cfg.UpstreamTimeout, s.cfg.ReconnectTimeout)
+    }
*** End Patch