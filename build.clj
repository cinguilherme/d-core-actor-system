(ns build
  (:require [clojure.java.shell :as sh]
            [clojure.string :as str]
            [clojure.tools.build.api :as b]
            [deps-deploy.deps-deploy :as dd]))

(def lib 'org.clojars.cinguilherme/d-core-actor-system)
(def version-file ".version")
(def class-dir "target/classes")
(def basis (b/create-basis {:project "deps.edn"}))

(defn read-version []
  (str/trim (slurp version-file)))

(defn parse-version [version]
  (when-let [[_ major minor patch] (re-matches #"(\d+)\.(\d+)\.(\d+)" version)]
    {:major (Long/parseLong major)
     :minor (Long/parseLong minor)
     :patch (Long/parseLong patch)}))

(defn format-version [{:keys [major minor patch]}]
  (str major "." minor "." patch))

(defn bump-version [version bump]
  (let [{:keys [major minor patch] :as parsed} (parse-version version)]
    (when-not parsed
      (throw (ex-info "Invalid version string in .version"
                      {:version version
                       :expected "MAJOR.MINOR.PATCH"})))
    (case bump
      :major (format-version {:major (inc major) :minor 0 :patch 0})
      :minor (format-version {:major major :minor (inc minor) :patch 0})
      :patch (format-version {:major major :minor minor :patch (inc patch)})
      (throw (ex-info "Invalid bump type"
                      {:bump bump
                       :allowed #{:major :minor :patch}})))))

(defn current-version []
  (read-version))

(defn jar-file [version]
  (format "target/%s-%s.jar" (name lib) version))

(defn write-version! [version]
  (spit version-file (str version "\n")))

(defn git! [& args]
  (let [{:keys [exit out err]} (apply sh/sh args)]
    (when-not (zero? exit)
      (throw (ex-info "Git command failed"
                      {:args args
                       :exit exit
                       :out out
                       :err err})))
    out))

(defn commit-version! [version]
  (git! "git" "add" version-file)
  (git! "git" "commit" "-m" (str "Bump version to " version) "--" version-file))

(defn pom-path []
  (b/pom-path {:lib lib :class-dir class-dir}))

(defn clean [_]
  (b/delete {:path "target"}))

(defn jar [_]
  (let [version (current-version)
        jar-file (jar-file version)]
    (clean nil)
    (b/copy-dir {:src-dirs ["src"] :target-dir class-dir})
    (b/write-pom {:class-dir class-dir
                  :lib lib
                  :version version
                  :basis basis
                  :src-dirs ["src"]
                  :pom-data
                  [[:licenses
                    [:license
                     [:name "MIT License"]
                     [:url "https://opensource.org/licenses/MIT"]]]
                   [:scm
                    [:url "https://github.com/cinguilherme/d-core-actor-system"]
                    [:connection "scm:git:https://github.com/cinguilherme/d-core-actor-system.git"]
                    [:developerConnection "scm:git:ssh://git@github.com/cinguilherme/d-core-actor-system.git"]]
                   [:developers
                    [:developer
                     [:id "cinguilherme"]
                     [:name "Guilherme Cintra"]]]]})
    (b/jar {:class-dir class-dir :jar-file jar-file})))

(defn deploy [_]
  (let [version (current-version)
        jar-file (jar-file version)]
    (jar nil)
    (dd/deploy {:installer :remote
                :sign-releases? false
                :artifact jar-file
                :pom-file (pom-path)})))

(defn publish [{:keys [bump]}]
  (let [bump (cond
               (keyword? bump) bump
               (string? bump) (keyword bump)
               :else nil)]
    (when-not bump
      (throw (ex-info "Missing bump type"
                      {:bump bump
                       :required #{:major :minor :patch}})))
    (let [current (current-version)
          next-version (bump-version current bump)]
      (println "Bumping version from" current "to" next-version)
      (write-version! next-version)
      (commit-version! next-version)
      (deploy nil))))
