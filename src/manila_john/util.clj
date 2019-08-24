(ns manila-john.util
  (:require [clojure.java.io :as io]
            [cemerick.uri :as uri])
  (:import java.lang.Class
           java.net.URLEncoder
           java.io.File))

(defn url
  "Thin layer on top of cemerick.uri/uri that defaults otherwise unqualified
   database urls to use `http://localhost:5984` and url-encodes each URL part
   provided."
  [& [base & parts :as args]]
  (try
    (apply uri/uri base (map (comp uri/uri-encode str) parts))
    (catch java.net.MalformedURLException e
      (apply uri/uri "http://localhost:5984"
        (map (comp uri/uri-encode str) args)))))

(defn server-url
  [db]
  (assoc db :path nil :query nil))

(defn get-mime-type
  [^File file]
  (java.net.URLConnection/guessContentTypeFromName (.getName file)))

;; TODO should be replaced with a java.io.Closeable Seq implementation and used
;; in conjunction with with-open on the client side
(defn read-lines
  "Like clojure.core/line-seq but opens f with reader.  Automatically
  closes the reader AFTER YOU CONSUME THE ENTIRE SEQUENCE.
  Pulled from clojure.contrib.io so as to avoid dependency on the old io
  namespace."
  [f]
  (let [read-line (fn this [^java.io.BufferedReader rdr]
                    (lazy-seq
                     (if-let [line (.readLine rdr)]
                       (cons line (this rdr))
                       (.close rdr))))]
    (read-line (io/reader f))))

(defmacro defn-wrap
  "Like defn, but applies wrap-fn."
  [name-sym wrap-fn & body]
  `(do
     (defn ~name-sym ~@body)
     (alter-var-root #'~name-sym ~wrap-fn)))

(defn update-alias-meta [old-name old-meta]
  (fn [new-meta]
    (merge new-meta
      (update old-meta :doc #(str "Alias for " old-name "." (if % "\n\n") %)))))

(defmacro defalias [new-name old-name]
  `(alter-meta!
    (def ~new-name)
      (update-alias-meta '~old-name (meta (var ~old-name)))))

(defmacro defaliases [& syms]
  `(do
    ~@(map
       (fn [[new-name old-name]]
         `(defalias ~new-name ~old-name))
       (partition 2 syms))))

(defn padded-hex [num]
  (let [s (Integer/toHexString num)]
    (if (even? (count s))
      s
      (str "0" s))))

(defn base16-encode [x]
  {:pre [(not (number? x))]}
  (->> (byte-array x)
       (map padded-hex)
       (apply str)))

(defn md5 [x]
  {:pre [(not (number? x))]}
  (-> (java.security.MessageDigest/getInstance "MD5")
      (.digest (.getBytes x))))

(def base16-md5 (comp base16-encode md5))
