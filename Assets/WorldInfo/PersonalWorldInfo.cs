using System.Collections.Generic;
using System.IO;
using UnityEngine;
using UnityEngine.Networking;
#if UNITY_EDITOR
using UnityEditor;
#endif

public class PersonalWorldInfo : MonoBehaviour
{
    [SerializeField] private string playerId;
    [SerializeField] private string cloudUrl = "https://example.com/{0}.json";
    [SerializeField] private List<WorldInfo> worlds = new List<WorldInfo>();

    private const string CacheDirectory = "Assets/WorldInfo/Cache";

    public string RequestUrl => string.IsNullOrEmpty(playerId) ? string.Empty : string.Format(cloudUrl, playerId);
    public IReadOnlyList<WorldInfo> Worlds => worlds;

    private void OnValidate()
    {
        if (!Application.isPlaying)
        {
            RefreshFromCloud();
        }
    }

    [ContextMenu("Refresh From Cloud")]
    public void RefreshFromCloud()
    {
        if (string.IsNullOrEmpty(RequestUrl))
        {
            Debug.LogWarning("PlayerId or cloudUrl not set");
            return;
        }

        try
        {
            using (UnityWebRequest req = UnityWebRequest.Get(RequestUrl))
            {
                var op = req.SendWebRequest();
                while (!op.isDone) { }

#if UNITY_2020_1_OR_NEWER
                if (req.result != UnityWebRequest.Result.Success)
#else
                if (req.isNetworkError || req.isHttpError)
#endif
                {
                    Debug.LogWarning($"Failed to download world info: {req.error}. Loading from cache.");
                    LoadFromCache();
                    return;
                }

                string json = req.downloadHandler.text;
                SaveCache(json);
                ParseJson(json);
            }
        }
        catch (System.Exception ex)
        {
            Debug.LogWarning($"Exception downloading world info: {ex.Message}. Loading from cache.");
            LoadFromCache();
        }
    }

    private void SaveCache(string json)
    {
        if (!Directory.Exists(CacheDirectory))
        {
            Directory.CreateDirectory(CacheDirectory);
        }
        File.WriteAllText(Path.Combine(CacheDirectory, playerId + ".json"), json);
    }

    private void LoadFromCache()
    {
        string path = Path.Combine(CacheDirectory, playerId + ".json");
        if (File.Exists(path))
        {
            string json = File.ReadAllText(path);
            ParseJson(json);
        }
    }

    private void ParseJson(string json)
    {
        if (string.IsNullOrEmpty(json))
        {
            worlds.Clear();
            return;
        }
        WorldList list = JsonUtility.FromJson<WorldList>("{\"items\":" + json + "}");
        worlds = list.items ?? new List<WorldInfo>();
    }

    [System.Serializable]
    public class WorldInfo
    {
        public string worldId;
        public string name;
        public string author;
        public string imageUrl;
        public string description;
        public string[] tags;
        public int visits;
        public int favorites;
        public string lastUpdate;
    }

    [System.Serializable]
    private class WorldList
    {
        public List<WorldInfo> items;
    }
}

#if UNITY_EDITOR
[CustomEditor(typeof(PersonalWorldInfo))]
public class PersonalWorldInfoEditor : Editor
{
    public override void OnInspectorGUI()
    {
        PersonalWorldInfo info = (PersonalWorldInfo)target;

        serializedObject.Update();
        EditorGUILayout.PropertyField(serializedObject.FindProperty("playerId"));
        EditorGUILayout.PropertyField(serializedObject.FindProperty("cloudUrl"));

        EditorGUILayout.Space();
        EditorGUILayout.LabelField("Request URL", info.RequestUrl);

        EditorGUILayout.Space();
        if (info.Worlds != null)
        {
            foreach (var w in info.Worlds)
            {
                EditorGUILayout.BeginVertical("box");
                EditorGUILayout.LabelField("Name", w.name);
                EditorGUILayout.LabelField("Visits", w.visits.ToString());
                EditorGUILayout.LabelField("Favorites", w.favorites.ToString());
                EditorGUILayout.LabelField("Last Update", w.lastUpdate);
                EditorGUILayout.EndVertical();
            }
        }

        if (GUILayout.Button("Refresh From Cloud"))
        {
            info.RefreshFromCloud();
        }

        serializedObject.ApplyModifiedProperties();
    }
}
#endif
