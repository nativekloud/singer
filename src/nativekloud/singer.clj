(ns nativekloud.singer
  (:require [cheshire.core :as json]
            [clojure.java.io :refer [writer]]
            [clojure.string :as s]
            [clojure.java.io :refer [delete-file]]
            [gcs.core :as gcs]))

;;; Encoding
(def content-type "application/json")

(defn encode [data]
  (json/generate-string data))

(defn decode [data]
  (json/parse-string data keyword))

;;; Multimethods


(defmulti tap
  (fn [args] (:type args)))

(defmulti sink
  (fn [args] (:type args)))

(defmulti discover
  (fn [args] (:type args)))

(defmulti transform
  (fn [args] (:type args)))


;;; Catalog

(defn ->CatalogEntry
  ([tap_stream_id stream schema]
   {:tap_stream_id tap_stream_id
    :stream stream
    :schema schema}))

(defn ->Catalog
  [streams]
  {:streams streams})


(defn stream [tap_stream_id stream schema]
  (->CatalogEntry tap_stream_id stream schema)
  )

(defn write-streams [streams]
  (println (encode (->Catalog streams))))

;;; Messages

(defn ->RecordMessage
  "RECORD message.
    The RECORD message has these fields:
      * stream (string) - The name of the stream the record belongs to.
      * record (map) - The raw data for the record
      * version (optional, int) - For versioned streams, the version
        number. Note that this feature is experimental and most Taps and
        Targets should not need to use versioned streams.
      * time_extracted (optional, date)

  (->RecordMessage \"users\" {:id 1 :name \"Mary\"})
  "
  ([stream record]
   {:type "RECORD"
    :stream stream
    :record record})
  ([stream record time_extracted]
   {:type "RECORD"
      :stream stream
      :record record
      :time_extracted time_extracted})
  ([stream record time_extracted version]
   {:type "RECORD"
    :stream stream
    :record record
    :version version
    :time_extracted time_extracted})
  )

(defn ->SchemaMessage
  "SCHEMA message.
    The SCHEMA message has these fields:
      * stream (string) - The name of the stream this schema describes.
      * schema (dict) - The JSON schema.
      * key_properties (list of strings) - List of primary key properties."
  ([stream schema key-properties]
   {:type "SCHEMA"
    :stream stream
    :schema schema
    :key-properties key-properties})
  ([stream schema key-properties bookmark-properties]
   {:type "SCHEMA"
    :stream stream
    :schema schema
    :key-properties key-properties
    :bookmark-properties bookmark-properties}))

(defn ->StateMessage
  "STATE message.
    The STATE message has one field:
      * value (dict) - The value of the state."
  [value]
  {:type"STATE"
   :value value})

(defn parse [s]
  "Parse a message string into a Message map."
  (let [m (decode s)]
    (when-not (map? m)
      (throw (Exception. "Message must be a map, got" s)))
    (case  (m "type")

      "RECORD"
      (->RecordMessage (m "stream")
                       (m "record")
                       (m "time_extracted")
                       (m "version"))

      "SCHEMA"
      (->SchemaMessage (m "stream")
                       (m "schema")
                       (m "key-properties")
                       (m "bookmark-properties"))

      "STATE"
      (->StateMessage (m "value")))))


(defn write-message [message]
  (println (encode message)))

(defn write-record [stream record time_extracted version]
  (write-message (->RecordMessage stream record time_extracted version)))

(defn write-records [stream records]
  (doseq [record records] (write-record stream record)))

(defn write-schema [stream schema key-properties bookmark-properties]
  (write-message (->SchemaMessage stream schema key-properties bookmark-properties)))

(defn write-state [value]
  (write-message (->StateMessage value)))


;;; Metadata
;; FIXME
;;; https://github.com/singer-io/getting-started/blob/master/docs/DISCOVERY_MODE.md#metadata
(def default {:selected                   false                   
              :replication-method         "INCREMENTAL" 
              :replication-key           nil
              :view-key-properties       []
              :inclusion                 "automatic"
              :selected-by-default       false
              :valid-replication-keys    []
              :schema-name               ""
              :forced-replication-method "FULL_TABLE"
              :table-key-properties      []
              :is-view                   false
              :row-count                 nil
              :database-name             ""
              :sql-datatype              nil
              })

(defn ->MetadataEntry
  ([]
   default)
  ([stream-name]
   (-> default
       (assoc :schema-name stream-name)
       (assoc :database-name stream-name))))


(defn metadata [stream]
  (->MetadataEntry (:stream stream)))


;;; Config

(defn url->parts [url]
  (let [[prefix path] (s/split url #"://")
        path-split (s/split path #"/" 2)]
    [(first path-split) (last path-split)]
    ))


(defmulti read-file (fn [path](keyword (first (s/split path #":")))))


(defmethod read-file :default [path]
   (decode (slurp path))
  )

(defmethod read-file :gs [path]
  (let [[bucket-name blob-name] (url->parts path)
        blob-content (gcs/get-blob-content bucket-name blob-name)]
    (if (nil? blob-content)
      blob-content
      (decode (String. blob-content )))
    )
  )

(defmulti save-file (fn [path _](keyword (first (s/split path #":")))))

(defmethod save-file :default [path data]
  (with-open [w (writer path)]
    (.write w (encode data))))

(defmethod save-file :gs [path data]
  (let [[bucket-name blob-name] (url->parts path)]
    (gcs/put-blob-string bucket-name blob-name content-type (encode  data))
    ))

(defmulti remove-file (fn [path](keyword (first (s/split path #":")))))

(defmethod remove-file :default [path]
  (delete-file path))

(defmethod remove-file :gs [path]
  (let [[bucket-name blob-name] (url->parts path)]
    (gcs/delete-blob bucket-name blob-name)))

(defn load-config
  "Reads config file and returns config map."
  [args]
  (->(:config args)
     read-file))

(defn save-config
  [args data]
  (save-file (:config args) data))

(defn read-state [args]
  (->(:state args)
     read-file))

(defn save-state [args data]
  (save-file (:state args) data))

(defn load-catalog [args]
  (->(:catalog args)
     read-file))

(defn save-catalog [args data]
  (save-file (:catalog args) data))
