using System.Collections.Generic;
using System.IO;
using TMPro;
using UnityEditor;
using UnityEngine;
#if VRC_SDK_VRCSDK3
using VRC.SDK3.Components.Image;
#endif

public class GenerateWorldCards
{
    [MenuItem("VRChat/Generate Personal Stats")]
    public static void Generate()
    {
        string path = Path.Combine(Application.dataPath, "user_worlds.json");
        if (!File.Exists(path))
        {
            Debug.LogError("user_worlds.json not found");
            return;
        }

        string json = File.ReadAllText(path);
        WorldList wrapper = JsonUtility.FromJson<WorldList>("{\"items\":" + json + "}");
        Debug.Log($"Loaded {wrapper.items.Count} worlds.");

        string templatePath = "Assets/WorldInfo/PersonalStatsTemplate.prefab";
        GameObject template = AssetDatabase.LoadAssetAtPath<GameObject>(templatePath);
        if (template == null)
        {
            Debug.LogError("Template prefab not found");
            return;
        }

        GameObject root = (GameObject)PrefabUtility.InstantiatePrefab(template);
        Transform content = root.transform.Find("ScrollRect/Content");
        if (content == null)
        {
            Debug.LogError("Content transform missing");
            Object.DestroyImmediate(root);
            return;
        }

        GameObject cardTemplate = content.GetChild(0).gameObject;

        foreach (WorldInfo world in wrapper.items)
        {
            GameObject card = Object.Instantiate(cardTemplate, content);

            foreach (TextMeshProUGUI tmp in card.GetComponentsInChildren<TextMeshProUGUI>())
            {
                if (tmp.name == "Title")
                {
                    tmp.text = world.name;
                }
                else if (tmp.name == "Visits")
                {
                    tmp.text = world.visits.ToString();
                }
            }

#if VRC_SDK_VRCSDK3
            VRCUrlImageDownloader downloader = card.GetComponentInChildren<VRCUrlImageDownloader>();
            if (downloader != null && !string.IsNullOrEmpty(world.imageUrl))
            {
                downloader.Url = world.imageUrl;
            }
#endif
        }

        Object.DestroyImmediate(cardTemplate);

        string savePath = "Assets/WorldInfo/PersonalStats.prefab";
        PrefabUtility.SaveAsPrefabAsset(root, savePath);
        Object.DestroyImmediate(root);
        AssetDatabase.SaveAssets();
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
    }

    [System.Serializable]
    public class WorldList
    {
        public List<WorldInfo> items;
    }
}
