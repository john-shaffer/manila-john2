(ns manila-john 
  "manila-john is a Clojure library for Apache CouchDB."
  (:require [manila-john.util :as util]
            [cemerick.uri :as uri]
            [cheshire.core :as json]
            [clojure.java.io :as io])
  (:use manila-john.http-client)
  (:import (java.io BufferedInputStream ByteArrayOutputStream File
                    FileInputStream InputStream)
           org.apache.commons.codec.binary.Base64))

(def ^{:private true} highest-supported-charcode 0xfff0)

(def ^{:doc "A very 'high' unicode character that can be used
              as a wildcard suffix when querying views."}
  ; \ufff0 appears to be the highest character that couchdb can support
  ; discovered experimentally with v0.10 and v0.11 ~March 2010
  ; now officially documented at http://wiki.apache.org/couchdb/View_collation
  wildcard-collation-string (str (char highest-supported-charcode)))

(def ^{:dynamic true :private true} *database* nil)

(def native-couch-fns #{"_approx_count_distinct" "_count" "_stats" "_sums"})

(defn with-db*
  [f]
  (fn [& [maybe-db & rest :as args]]
    (if (and (thread-bound? #'*database*)
             (not (identical? maybe-db *database*)))
    (apply f *database* args)
    (apply f (util/url maybe-db) rest))))

(defmacro with-db
  "Takes a URL or database name (useful for localhost only).  That value is used
   to configure the subject of all of the operations within the dynamic scope of
   the body."
  [database & body]
  `(binding [*database* (util/url ~database)]
     ~@body))

(defmacro with-test-db
  "Creates a randomly named test db on $MJ_TEST_SERVER or localhost:5984, runs
   the body inside with-db, and finally deletes the db."
  [& body]
  `(let [db# (str (or (System/getenv "MJ_TEST_SERVER") "http://localhost:5984")
                  "/test-" (java.util.UUID/randomUUID))]
     (try
       (create-db db#)
       (with-db db#
         ~@body)
       (finally
         (delete-db db#)))))

(defn wrap-db-op [f]
  (fn [& [maybe-db & rest :as args]]
    (let [db-var #'manila-john/*database*
          db @db-var]
      (if (and (thread-bound? db-var)
               (not (identical? maybe-db db)))
        (apply f db args)
        (apply f (util/url maybe-db) rest)))))

(defn wrap-db-op*
  "Like wrap-db-op, but doesn't pass db to the wrapped fn. Instead it ensures
   that the db is bound to manila-john/*database*, so that the db does not need
   to be passed to most manila-john fns."
  [f]
  (wrap-db-op
   #(with-db %
      (apply f %&))))

(defmacro defdbop
  "Same as defn, but wraps the defined function in another that transparently
   allows for dynamic or explicit application of database configuration as well
   as implicit coercion of the first `db` argument to a URL instance."
  [name-sym & body]
  `(do
     (util/defn-wrap ~name-sym wrap-db-op ~@body)
     (alter-meta! (var ~name-sym) update-in [:doc] str
       "\n\n  When used within the dynamic scope of `with-db`, the initial `db`"
       "\n  argument is automatically provided.")))

(defmacro defdbop*
  "Like defdbop, but using wrap-db-op*."
  [name-sym & body]
  `(do
     (util/defn-wrap ~name-sym wrap-db-op* ~@body)
     (alter-meta! #'~name-sym update-in [:doc] str
                  "\n\n Wrapped in wrap-db-op*.")))

(defmacro dbop
  "Same as fn wrapped in wrap-db-op."
  [& body]
  `(wrap-db-op (fn ~@body)))

(defmacro dbop*
  "Same as fn wrapped in wrap-db-op*."
  [& body]
  `(wrap-db-op* (fn ~@body)))

(defdbop couchdb-info
  "Returns information about a CouchDB instance."
  [db]
  (couchdb-request :get (util/server-url db)))

(defdbop all-dbs
  "Return a list of all databases on the CouchDB server."
  [db]
  (couchdb-request :get (uri/uri db "/_all_dbs")))

(defdbop create-db
  "Create the database."
  [db]
  (couchdb-request :put db)
  db)

(util/defalias put-db create-db)

(defdbop db-info
  "Return the database's meta information."
  [db]
  (couchdb-request :get db))

(defdbop get-db
  "Create the database if it doesn't exist, and return the database's meta
   information (as db-info) whether it existed or not."
  [db]
  (merge db
         (or (db-info db)
             (and (create-db db)
                  (db-info db)))))

(defdbop delete-db
  "Delete the database."
  [db]
  (couchdb-request :delete db))

(defdbop replicate-db
  "Takes two arguments (a source and target for replication) which could be a
   string (name of a database in the default configuration) or a map that
   contains a least the database name (':name' keyword, map is merged with
   default configuration) and reproduces all the active documents in the
   source database on the target databse."
  [srcdb tgtdb]
  (couchdb-request :post
    (uri/uri tgtdb "/_replicate")
    :data {:source (str srcdb)
           :target (str tgtdb)}))

(def ^{:private true} byte-array-class (Class/forName "[B"))

(defn attachment-info
  ([{:keys [data filename mime-type data-length]}]
    (attachment-info data data-length filename mime-type))
  ([data data-length filename mime-type]
    (let [data (if (string? data)
                 (File. ^String data)
                 data)
          check (fn [k v]
                  (if v v
                    (throw (IllegalArgumentException.
                             (str k " must be provided if attachment data is an InputStream or byte[]")))))]
      (cond
        (instance? File data)
          [(-> ^File data FileInputStream. BufferedInputStream.)
           (.length ^File data)
           (or filename (.getName ^File data))
           (or mime-type (util/get-mime-type data))]

        (instance? InputStream data)
          [data (check :data-length data-length)
           (check :filename filename) (check :mime-type mime-type)]

        (= byte-array-class (class data))
          [(java.io.ByteArrayInputStream. data) (count data)
           (check :filename filename) (check :mime-type mime-type)]

        :default
          (throw (IllegalArgumentException.
                   (str "Cannot handle attachment data of type "
                        (class data))))))))

(defn to-byte-array
  [input]
  (if (= byte-array-class (class input))
    input
    ; make sure streams are closed so we don't hold locks on files on Windows
    (with-open [^InputStream input (io/input-stream input)]
      (let [out (ByteArrayOutputStream.)]
        (io/copy input out)
        (.toByteArray out)))))

(defdbop put-doc
  [db document & {:keys [id attachments request-params]}]
  (let [document (merge document
                        (when id {:_id id})
                        (when (seq attachments)
                          (->> attachments
                            (map #(if (map? %) % {:data %}))
                            (map attachment-info)
                            (reduce (fn [m [data data-length filename mime]]
                                      (assoc m (keyword filename)
                                        {:content_type mime
                                         :data (-> data
                                                 to-byte-array
                                                 Base64/encodeBase64String)}))
                                    {})
                            (hash-map :_attachments))))
        result (couchdb-request
                 (if (:_id document) :put :post)
                 (assoc (util/url db (:_id document))
                   :query request-params)
                 :data document)]
    (and (:ok result)
      (assoc document :_rev (:rev result) :_id (:id result)))))

(defn dissoc-meta
  "dissoc'es the :_id and :_rev slots from the provided map."
  [doc]
  (dissoc doc :_id :_rev))

(defdbop get-doc
  "Returns the document identified by the given id. Optional CouchDB document
  API query parameters may be provided as keyword arguments."
  [db id & {:as get-params}]
  ;; TODO a nil or empty key should probably just throw an exception
  (when (seq (str id))
    (couchdb-request :get
      (-> (util/url db id)
        (assoc :query get-params)))))

(defdbop doc-exists?
  "Returns true if a document with the given key exists in the database."
  [db id]
  ;; TODO a nil or empty key should probably just throw an exception
  (when (seq (str id))
    (= 200 (:status (couchdb-request* :head (util/url db id))))))

(defn doc? [x]
  (and (map? x) (:_id x) (:_rev x)))

(defn doc-url
  [database-url document]
  (when-not (:_id document)
    (throw (IllegalArgumentException.
             "A valid document with an :_id slot is required.")))
  (let [with-id (util/url database-url (:_id document))]
    (if-let [rev (:_rev document)]
      (assoc with-id :query (str "rev=" rev))
      with-id)))

(defdbop delete-doc
  "Takes a document and deletes it from the database."
  [db document]
  (couchdb-request :delete (doc-url db document)))

(defn doc-slug [doc]
  (str (:_id doc) (when (:_rev doc) (str "?rev=" (:_rev doc)))))

(defdbop copy-doc
  "Copies the provided document (or the document with the given string id)
   to the given new id.  If the destination id identifies an existing
   document, then a document map (with up-to-date :_id and :_rev slots)
   must be provided as a destination argument to avoid a 409 Conflict."
  [db src dest]
  (let [dest (if (map? dest) dest {:_id dest})
        dest (doc-slug dest)]
    (couchdb-request :copy
      (doc-url db (if (map? src) src {:_id src}))
      :headers {"Destination" dest})))

(defdbop configure-view-server
  "Sets the query server exec string for views written in the specified
   :language (\"clojure\" by default).  This is intended to be a REPL
   convenience function; see the README for more info."
  [db exec-string & {:keys [language] :or {language "clojure"}}]
  (couchdb-request :put
    (uri/uri db "/_config/query_servers/" (-> language name uri/uri-encode))
    :data (pr-str exec-string)))

(defn map-leaves [f m]
  (into {} (for [[k v] m]
             (if (map? v)
               [k (map-leaves f v)]
               [k (f v)]))))

(defmulti view-transformer identity)

(defmethod view-transformer :clojure
  [_]
  {:language :clojure
   :compiler (fn [options] pr-str)})

(defmethod view-transformer :default
  [language]
  {:language language
   :compiler (fn [options] str)})

(defmethod view-transformer :cljs
  [language]
  (try
    (require 'manila-john.cljs-views)
    ; manila-john.cljs-views defines a method for :cljs, so this
    ; call will land in it
    (view-transformer language)
    (catch Exception e
      (throw (UnsupportedOperationException.
               (str "Could not load manila-john.cljs-views; ensure "
                    "ClojureScript and its dependencies are available, and that "
                    "you're using Clojure >= 1.3.0.") e)))))

(defmethod view-transformer :clojurescript
  [_]
  (view-transformer :cljs))

(defn view-language-options
  "Return [language other-options]."
  [options]
  (if (keyword? options)
    [options nil]
    [(or (:language options) :javascript) (dissoc options :language)]))

(defn view-server-fns-fn
  [options fns]
  (let [[language options] (view-language-options options)
        {:keys [compiler language]} (view-transformer language)
        compiler (compiler options)
        compiler #(or (native-couch-fns %) (compiler %))
        fns (map-leaves compiler fns)]
    [language fns]))

(defmacro view-server-fns
  [options fns]
  (let [[language options] (if (map? options)
                             [(or (:language options) :javascript)
                                  (dissoc options :language)]
                             [options])]
    [(:language (view-transformer language))
     `(#'map-leaves
        ((:compiler (view-transformer ~language)) ~options)
        '~fns)]))

(defdbop save-design-doc
  "Create/update a design document containing functions used for database
   queries/filtering/validation/etc."
  [db fn-type design-document-name [language view-server-fns]]
  (let [design-doc-id (str "_design/" design-document-name)
        ddoc {fn-type view-server-fns
              :language (name language)}]
    (if-let [design-doc (get-doc db design-doc-id)]
      (put-doc db (merge design-doc ddoc))
      (put-doc db (assoc ddoc :_id design-doc-id)))))

(defdbop save-view
  "Create or update a design document containing views used for queries."
  [db & args]
  (apply save-design-doc db :views args))

(defdbop save-filter
  "Create a filter for use with CouchDB change notifications API."
  [db & args]
  (apply save-design-doc db :filters args))

(defn- get-view*
  "Get documents associated with a design document. Also takes an optional map
   for querying options, and a second map of {:key [keys]} to be POSTed.
   (see: http://wiki.apache.org/couchdb/HTTP_view_API)."
  [db path-segments & [query-params-map post-data-map]]
  (let [url (assoc (apply util/url db path-segments)
              :query (into {} (for [[k v] query-params-map]
                                [k (if (#{"key" "keys" "startkey" "endkey"}
                                          (name k))
                                     (json/generate-string v)
                                     v)])))]
    (view-request
      (if (empty? post-data-map) :get :post)
      url
      :data (when (seq post-data-map) post-data-map))))

(defdbop get-view
  "Get documents associated with a design document. Also takes an optional map
   for querying options, and a second map of {:key [keys]} to be POSTed.
   (see: http://wiki.apache.org/couchdb/HTTP_view_API)."
  [db design-document view-key & [query-params-map post-data-map :as args]]
  (apply get-view* db
         ["_design" design-document "_view" (name view-key)]
         args))

(defdbop all-docs
  "Returns the meta (_id and _rev) of all documents in a database. By adding
   the key ':include_docs' with a value of true to the optional query params map
   you can also get the full documents, not just their meta. Also takes an
   optional second map of {:keys [keys]} to be POSTed.
   (see: http://wiki.apache.org/couchdb/HTTP_view_API)."
  [db & [query-params-map post-data-map :as args]]
  (apply get-view* db ["_all_docs"] args))

(defdbop ad-hoc-view
  "One-off queries (i.e. views you don't want to save in the CouchDB database).
   Ad-hoc views should be used only during development. Also takes an optional
   map for querying options
   (see: http://wiki.apache.org/couchdb/HTTP_view_API)."
  [db [language view-server-fns] & [query-params-map]]
  (get-view* db ["_temp_view"] query-params-map
    (into {:language language} view-server-fns)))

(defdbop bulk-update
  "Takes a sequential collection of documents (maps) and inserts or updates
   (if \"_id\" and \"_rev\" keys are supplied in a document) them with in a
   single request. Optional keyword args may be provided, and are sent along
   with the documents (e.g. for \"all-or-nothing\" semantics, etc)."
  [db documents & {:as options}]
  (couchdb-request :post
    (util/url db "_bulk_docs")
    :data (assoc options :docs documents)))

(defdbop put-attachment
  "Updates (or creates) the attachment for the given document.  `data` can be a
   string path to a file, a java.io.File, a byte array, or an InputStream.

   If `data` is a byte array or InputStream, you _must_ include the following
   otherwise-optional
   kwargs:
       :filename — name to be given to the attachment in the document
       :mime-type — type of attachment data
   These are derived from a file path or File if not provided.  (Mime types are
   derived from filename extensions; see manila-john.util/get-mime-type for
   determining mime type yourself from a File object.)"
  [db document data & {:keys [filename mime-type data-length]}]
  (let [[stream data-length filename mime-type]
         (attachment-info data data-length filename mime-type)]
    (couchdb-request :put
      (-> db
        (doc-url document)
        (util/url (name filename)))
      :data stream
      :data-length data-length
      :content-type mime-type)))

(defdbop delete-attachment
  "Deletes an attachment from a document."
  [db document file-name]
  (couchdb-request :delete (util/url (doc-url db document) file-name)))

(defdbop get-attachment
  "Returns an InputStream reading the named attachment to the specified/provided
   document or nil if the document or attachment does not exist.
   Hint: use the copy or to-byte-array fns in c.c.io to easily redirect the
   result."
  [db doc-or-id attachment-name]
  (let [doc (if (map? doc-or-id) doc-or-id (get-doc db doc-or-id))
        attachment-name (if (keyword? attachment-name)
                          (name attachment-name)
                          attachment-name)]
    (when (-> doc :_attachments (get (keyword attachment-name)))
      (couchdb-request :get
                       (-> (doc-url db doc)
                         (util/url attachment-name)
                         (assoc :as :stream))))))

(defdbop changes
  "Returns a lazy seq of the rows in _changes, as configured by the given
   options. If you want to react to change notifications, you should probably
   use `change-agent`."
  [db & {:keys [since limit descending feed heartbeat
                timeout filter include_docs style] :as opts}]
  (let [url (uri/uri db "_changes")
        response (couchdb-request* :get (assoc url :query opts :as :stream))]
    (when-not response
      (throw (IllegalStateException.
               (str "Database for _changes feed does not exist: " url))))
    (-> response
      :body
      (lazy-view-seq (not= "continuous" feed))
      (vary-meta assoc ::http-resp response))))

(defn- change-agent-config
  [db options]
  (merge {:heartbeat 30000 :feed "continuous"}
         options
         (when-not (:since options)
           {:since (:update_seq (db-info db))})
         {::db db
          ::state :init
          ::last-update-seq nil}))

(defdbop change-agent
  "Returns an agent whose state will very to contain events
   emitted by the _changes feed of the specified database.
   Users are expected to attach functions to the agent using
   `add-watch` in order to be notified of changes.  The
   returned change agent is entirely 'managed', with
   `start-changes` and `stop-changes` controlling its operation
   and sent actions.  If you send actions to a change agent,
   bad things will likely happen."
  [db & {:as opts}]
  (agent nil :meta {::changes-config (atom (change-agent-config db opts))}))

(defn- run-changes
  [_]
  (let [config-atom (-> *agent* meta ::changes-config)
        config @config-atom]
    (case (::state config)
      :init (let [changes (apply changes (::db config)
                            (flatten (remove (comp namespace key) config)))
                  http-resp (-> changes meta ::http-resp)]
              ; cannot shut down continuous _changes feeds without aborting this
              (assert (-> http-resp :request :http-req))
              (swap! config-atom merge {::seq changes
                                        ::http-resp http-resp
                                        ::state :running})
              (send-off *agent* #'run-changes)
              nil)
      :running (let [change-seq (::seq config)
                     change (first change-seq)
                     last-change-seq (or (:seq change) (:last_seq change))]
                 (send-off *agent* #'run-changes)
                 (when-not (= :stopped (::state @config-atom))
                   (swap! config-atom merge
                          {::seq (rest change-seq)}
                          (when last-change-seq
                            {::last-update-seq last-change-seq})
                          (when-not change {::state :stopped}))
                   change))
      :stopped (-> config ::http-resp :request :http-req .abort))))

(defn changes-running?
  "Returns true only if the given change agent has been started
   (using `start-changes`) and is delivering changes to
   attached watches."
  [^clojure.lang.Agent change-agent]
  (boolean (-> change-agent meta ::state #{:init :running})))

(defn start-changes
  "Starts the flow of changes through the given change-agent.
   All of the options provided to `change-agent` are used to
   configure the underlying _changes feed."
  [change-agent]
  (send-off change-agent #'run-changes))

(defn stop-changes
  "Stops the flow of changes through the given change-agent.
   Change agents can be restarted with `start-changes`."
  [change-agent]
  (swap! (-> change-agent meta ::changes-config) assoc ::state :stopped)
  change-agent)

(defn fixture-with-docs
  "Take a collection of docs and options to bulk-update, and return a
   clojure.test fixture fn that loads the docs and runs its test inside
   with-test-db."
  [docs & options]
  #(with-test-db
     (apply bulk-update docs options)
     (%)))

(defdbop get-or-save-view [db ddoc-name view-key view-language compiled-view-fns
                           & [query-params-map post-data-map :as args]]
  (try
    (apply get-view db ddoc-name view-key args)
    (catch Exception e
      (let [ddoc-id (str "_design/" (uri/uri-encode ddoc-name))
            ddoc (or (get-doc db ddoc-id) {:_id ddoc-id})]
        (if (= compiled-view-fns (:views ddoc))
          (throw e)
          (do
            (->> (assoc ddoc :language (name view-language)
                   :views compiled-view-fns)
                 (put-doc db))
            (apply get-view db ddoc-name view-key args)))))))

(defdbop* bulk-get [keys & [query-params-map]]
  (-> (merge query-params-map {:include_docs true})
      (all-docs {:keys keys})
      (->> (map :doc ))))

(defmacro defviews [options view-compiler-options & views]
  "Usage: (defviews \"forum\" :javascript
           (by-uri
            \"function (doc) {
                              emit (doc.uri)}\"
                              \"_count\"))
          (by-uri db {:key \"/asd\"})"
  (let [options (if (string? options) {:ddoc-prefix options} options)
        view-map (fn [[sym mapper reducer]]
                   {(keyword (name sym))
                    (if reducer ; :reduce nil makes Cloudant choke
                      {:map mapper
                       :reduce reducer}
                      {:map mapper})})
        [view-language] (view-language-options view-compiler-options)
        view-fns (apply merge (map view-map views))
        [compiled-view-language compiled-view-fns]
          (view-server-fns-fn view-compiler-options view-fns)
        ddoc-name (or (:ddoc-name options)
                      (str (or (:ddoc-prefix options) "manila-john-auto-")
                      (hash view-fns)))
        sym-meta {:ddoc-name ddoc-name
                  :view-language view-language
                  :view-fns view-fns
                  :compiled-view-language compiled-view-language
                  :compiled-view-fns compiled-view-fns}
        clj-fn (fn [[sym]]
                 (let [sym-meta (assoc sym-meta :view-name (name sym))
                       sym (with-meta sym {:manila-john sym-meta})]
                   `(defdbop* ~sym [& args#]
                      (apply get-or-save-view ~ddoc-name ~(keyword (name sym))
                             ~compiled-view-language ~compiled-view-fns
                             args#))))]
    `(do ~@(map clj-fn views))))

(defn add-missing-view-keys
  "Assoc keys as :startkey and :endkey depending on (:descending options) if
   they are not already present."
  [low-key high-key options]
  (let [[a b] (if (:descending options)
                [:endkey :startkey]
                [:startkey :endkey])]
    (merge {a low-key, b high-key} options)))
